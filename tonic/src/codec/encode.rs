use super::compression::{
    CompressionEncoding, CompressionSettings, SingleMessageCompressionOverride, compress,
};
use super::{
    AsyncEncode, BufferSettings, DEFAULT_MAX_SEND_MESSAGE_SIZE, EncodeBuffer, Encoder, HEADER_SIZE,
};
use crate::Status;
use bytes::{BufMut, Bytes, BytesMut};
use http::HeaderMap;
use http_body::{Body, Frame};
use pin_project::pin_project;
use std::{
    pin::Pin,
    task::{Context, Poll, ready},
};
use tokio_stream::{Stream, StreamExt, adapters::Fuse};

#[doc(hidden)]
#[pin_project]
pub struct EncodedBytes<T, U>
where
    T: Encoder<Error = Status> + 'static,
    U: Stream<Item = Result<T::Item, Status>>,
{
    #[pin]
    source: Fuse<U>,
    #[pin]
    encoder: T,
    compression_encoding: Option<CompressionEncoding>,
    max_message_size: Option<usize>,
    buffer_settings: BufferSettings,
    buf: BytesMut,
    uncompression_buf: BytesMut,
    #[pin]
    in_flight: Option<T::Encode>,
    in_flight_offset: Option<usize>,
    error: Option<Status>,
}

impl<T, U> EncodedBytes<T, U>
where
    T: Encoder<Error = Status> + 'static,
    U: Stream<Item = Result<T::Item, Status>>,
{
    #[inline]
    fn new(
        encoder: T,
        source: U,
        compression_encoding: Option<CompressionEncoding>,
        compression_override: SingleMessageCompressionOverride,
        max_message_size: Option<usize>,
    ) -> Self {
        let buffer_settings = encoder.buffer_settings();
        let buf = BytesMut::with_capacity(buffer_settings.buffer_size);

        let compression_encoding =
            if compression_override == SingleMessageCompressionOverride::Disable {
                None
            } else {
                compression_encoding
            };

        let uncompression_buf = if compression_encoding.is_some() {
            BytesMut::with_capacity(buffer_settings.buffer_size)
        } else {
            BytesMut::new()
        };

        EncodedBytes {
            source: source.fuse(),
            encoder,
            compression_encoding,
            max_message_size,
            buffer_settings,
            buf,
            uncompression_buf,
            in_flight: None,
            in_flight_offset: None,
            error: None,
        }
    }
    #[inline]
    fn start_encoding(mut self: Pin<&mut Self>, item: T::Item) -> Result<(), Status> {
        let mut this = self.as_mut().project();
        debug_assert!(this.in_flight.as_ref().get_ref().is_none());
        debug_assert!(this.in_flight_offset.is_none());

        let offset = this.buf.len();
        let compression_encoding = *this.compression_encoding;

        this.buf.reserve(HEADER_SIZE);
        unsafe {
            this.buf.advance_mut(HEADER_SIZE);
        }

        let dst = if compression_encoding.is_some() {
            this.uncompression_buf.clear();
            EncodeBuffer::new(std::mem::take(this.uncompression_buf))
        } else {
            EncodeBuffer::new(std::mem::take(this.buf))
        };

        let encode = this.encoder.encode(item, dst)?;
        this.in_flight.set(Some(encode));
        *this.in_flight_offset = Some(offset);

        Ok(())
    }

    #[inline]
    fn poll_encode(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Status>> {
        if self
            .as_ref()
            .project_ref()
            .in_flight
            .as_ref()
            .get_ref()
            .is_none()
        {
            return Poll::Ready(Ok(()));
        }

        let result = {
            let this = self.as_mut().project();
            let encode = this
                .in_flight
                .as_pin_mut()
                .expect("encode operation must be in-flight");
            ready!(encode.poll_encode(cx))
        };

        let mut this = self.as_mut().project();
        this.in_flight.set(None);
        let offset = this
            .in_flight_offset
            .take()
            .expect("encode completion must have in-flight offset");
        let compression_encoding = *this.compression_encoding;

        match result {
            Ok(encode_buffer) => Poll::Ready(finish_encode_buffer(
                encode_buffer,
                this.buf,
                this.uncompression_buf,
                this.buffer_settings,
                this.max_message_size,
                offset,
                compression_encoding,
            )),
            Err(status) => Poll::Ready(Err(status)),
        }
    }

    #[inline]
    fn take_buf_if_over_threshold(mut self: Pin<&mut Self>) -> Option<Bytes> {
        let this = self.as_mut().project();
        if !this.buf.is_empty() && this.buf.len() >= this.buffer_settings.yield_threshold {
            Some(take_buf(this.buf))
        } else {
            None
        }
    }
}
#[inline]
fn finish_encode_buffer(
    encode_buffer: EncodeBuffer,
    buf: &mut BytesMut,
    uncompression_buf: &mut BytesMut,
    buffer_settings: &BufferSettings,
    max_message_size: &Option<usize>,
    offset: usize,
    compression_encoding: Option<CompressionEncoding>,
) -> Result<(), Status> {
    if compression_encoding.is_some() {
        *uncompression_buf = encode_buffer.into_inner();
    } else {
        *buf = encode_buffer.into_inner();
    }

    finish_encoded_item(
        buf,
        uncompression_buf,
        *buffer_settings,
        *max_message_size,
        offset,
        compression_encoding,
    )
}

#[inline]
fn finish_encoded_item(
    buf: &mut BytesMut,
    uncompression_buf: &mut BytesMut,
    buffer_settings: BufferSettings,
    max_message_size: Option<usize>,
    offset: usize,
    compression_encoding: Option<CompressionEncoding>,
) -> Result<(), Status> {
    if let Some(encoding) = compression_encoding {
        let uncompressed_len = uncompression_buf.len();

        compress(
            CompressionSettings {
                encoding,
                buffer_growth_interval: buffer_settings.buffer_size,
            },
            uncompression_buf,
            buf,
            uncompressed_len,
        )
        .map_err(|err| Status::internal(format!("Error compressing: {err}")))?;
    }

    // now that we know length, we can write the header
    finish_encoding(compression_encoding, max_message_size, &mut buf[offset..])
}

impl<T, U> Stream for EncodedBytes<T, U>
where
    T: Encoder<Error = Status> + 'static,
    U: Stream<Item = Result<T::Item, Status>>,
{
    type Item = Result<Bytes, Status>;

    #[inline]
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            if self
                .as_ref()
                .project_ref()
                .in_flight
                .as_ref()
                .get_ref()
                .is_some()
            {
                match self.as_mut().poll_encode(cx) {
                    Poll::Ready(Ok(())) => {}
                    Poll::Ready(Err(status)) => return Poll::Ready(Some(Err(status))),
                    Poll::Pending => return Poll::Pending,
                }
            }

            if let Some(bytes) = self.as_mut().take_buf_if_over_threshold() {
                return Poll::Ready(Some(Ok(bytes)));
            }

            if let Some(status) = self.as_mut().project().error.take() {
                return Poll::Ready(Some(Err(status)));
            }

            let item = {
                let mut this = self.as_mut().project();
                match this.source.as_mut().poll_next(cx) {
                    Poll::Pending if this.buf.is_empty() => return Poll::Pending,
                    Poll::Ready(None) if this.buf.is_empty() => return Poll::Ready(None),
                    Poll::Pending | Poll::Ready(None) => {
                        return Poll::Ready(Some(Ok(take_buf(this.buf))));
                    }
                    Poll::Ready(Some(Ok(item))) => item,
                    Poll::Ready(Some(Err(status))) => {
                        if this.buf.is_empty() {
                            return Poll::Ready(Some(Err(status)));
                        }

                        *this.error = Some(status);
                        return Poll::Ready(Some(Ok(take_buf(this.buf))));
                    }
                }
            };

            if let Err(status) = self.as_mut().start_encoding(item) {
                return Poll::Ready(Some(Err(status)));
            }
        }
    }
}

impl<T, U> std::fmt::Debug for EncodedBytes<T, U>
where
    T: Encoder<Error = Status> + 'static,
    U: Stream<Item = Result<T::Item, Status>>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EncodedBytes").finish()
    }
}

#[inline]
fn take_buf(buf: &mut BytesMut) -> Bytes {
    buf.split_to(buf.len()).freeze()
}

#[inline]
fn finish_encoding(
    compression_encoding: Option<CompressionEncoding>,
    max_message_size: Option<usize>,
    buf: &mut [u8],
) -> Result<(), Status> {
    let len = buf.len() - HEADER_SIZE;
    let limit = max_message_size.unwrap_or(DEFAULT_MAX_SEND_MESSAGE_SIZE);
    if len > limit {
        return Err(Status::out_of_range(format!(
            "Error, encoded message length too large: found {len} bytes, the limit is: {limit} bytes"
        )));
    }

    if len > u32::MAX as usize {
        return Err(Status::resource_exhausted(format!(
            "Cannot return body with more than 4GB of data but got {len} bytes"
        )));
    }
    {
        let mut buf = &mut buf[..HEADER_SIZE];
        buf.put_u8(compression_encoding.is_some() as u8);
        buf.put_u32(len as u32);
    }

    Ok(())
}

#[derive(Debug)]
enum Role {
    Client,
    Server,
}

/// A specialized implementation of [Body] for encoding [Result<Bytes, Status>].
#[pin_project]
#[derive(Debug)]
pub struct EncodeBody<S = ()> {
    #[pin]
    inner: S,
    state: EncodeState,
}

#[derive(Debug)]
struct EncodeState {
    error: Option<Status>,
    role: Role,
    is_end_stream: bool,
}

impl EncodeBody<()> {
    /// Turns a stream of grpc messages into [EncodeBody] which is used by grpc clients for
    /// turning the messages into http frames for sending over the network.
    #[inline]
    pub fn new_client<T, U>(
        encoder: T,
        source: U,
        compression_encoding: Option<CompressionEncoding>,
        max_message_size: Option<usize>,
    ) -> EncodeBody<EncodedBytes<T, U>>
    where
        T: Encoder<Error = Status> + 'static,
        U: Stream<Item = Result<T::Item, Status>>,
    {
        EncodeBody {
            inner: EncodedBytes::new(
                encoder,
                source,
                compression_encoding,
                SingleMessageCompressionOverride::default(),
                max_message_size,
            ),
            state: EncodeState {
                error: None,
                role: Role::Client,
                is_end_stream: false,
            },
        }
    }

    /// Turns a stream of grpc results (message or error status) into [EncodeBody] which is used by grpc
    /// servers for turning the messages into http frames for sending over the network.
    #[inline]
    pub fn new_server<T, U>(
        encoder: T,
        source: U,
        compression_encoding: Option<CompressionEncoding>,
        compression_override: SingleMessageCompressionOverride,
        max_message_size: Option<usize>,
    ) -> EncodeBody<EncodedBytes<T, U>>
    where
        T: Encoder<Error = Status> + 'static,
        U: Stream<Item = Result<T::Item, Status>>,
    {
        EncodeBody {
            inner: EncodedBytes::new(
                encoder,
                source,
                compression_encoding,
                compression_override,
                max_message_size,
            ),
            state: EncodeState {
                error: None,
                role: Role::Server,
                is_end_stream: false,
            },
        }
    }
}

impl EncodeState {
    #[inline]
    fn trailers(&mut self) -> Option<Result<HeaderMap, Status>> {
        match self.role {
            Role::Client => None,
            Role::Server => {
                if self.is_end_stream {
                    return None;
                }

                self.is_end_stream = true;
                let status = if let Some(status) = self.error.take() {
                    status
                } else {
                    Status::ok("")
                };
                Some(status.to_header_map())
            }
        }
    }
}

impl<S> Body for EncodeBody<S>
where
    S: Stream<Item = Result<Bytes, Status>>,
{
    type Data = Bytes;
    type Error = Status;

    #[inline]
    fn is_end_stream(&self) -> bool {
        self.state.is_end_stream
    }

    #[inline]
    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let self_proj = self.project();
        match ready!(self_proj.inner.poll_next(cx)) {
            Some(Ok(d)) => Some(Ok(Frame::data(d))).into(),
            Some(Err(status)) => match self_proj.state.role {
                Role::Client => Some(Err(status)).into(),
                Role::Server => {
                    self_proj.state.is_end_stream = true;
                    Some(Ok(Frame::trailers(status.to_header_map()?))).into()
                }
            },
            None => self_proj
                .state
                .trailers()
                .map(|t| t.map(Frame::trailers))
                .into(),
        }
    }
}
