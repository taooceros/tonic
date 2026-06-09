use prost::Message;
use std::{
    future::{Ready, ready},
    marker::PhantomData,
    pin::Pin,
};
use tonic::Status;
use tonic::codec::{BufferSettings, Codec, DecodeBuf, Decoder, EncodeBuffer, Encoder, ReadyEncode};

/// A [`Codec`] that implements `application/grpc+proto` via the prost library.
#[derive(Debug, Clone)]
pub struct ProstCodec<T, U> {
    _pd: PhantomData<(T, U)>,
}

impl<T, U> ProstCodec<T, U> {
    /// Configure a ProstCodec with encoder/decoder buffer settings. This is used to control
    /// how memory is allocated and grows per RPC.
    pub fn new() -> Self {
        Self { _pd: PhantomData }
    }
}

impl<T, U> Default for ProstCodec<T, U> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T, U> ProstCodec<T, U>
where
    T: Message + Send + 'static,
    U: Message + Default + Send + 'static,
{
    /// A tool for building custom codecs based on prost encoding and decoding.
    /// See the codec_buffers example for one possible way to use this.
    pub fn raw_encoder(buffer_settings: BufferSettings) -> <Self as Codec>::Encoder {
        ProstEncoder {
            _pd: PhantomData,
            buffer_settings,
        }
    }

    /// A tool for building custom codecs based on prost encoding and decoding.
    /// See the codec_buffers example for one possible way to use this.
    pub fn raw_decoder(buffer_settings: BufferSettings) -> <Self as Codec>::Decoder {
        ProstDecoder {
            _pd: PhantomData,
            buffer_settings,
        }
    }
}

impl<T, U> Codec for ProstCodec<T, U>
where
    T: Message + Send + 'static,
    U: Message + Default + Send + 'static,
{
    type Encode = T;
    type Decode = U;

    type Encoder = ProstEncoder<T>;
    type Decoder = ProstDecoder<U>;

    fn encoder(&mut self) -> Self::Encoder {
        ProstEncoder {
            _pd: PhantomData,
            buffer_settings: BufferSettings::default(),
        }
    }

    fn decoder(&mut self) -> Self::Decoder {
        ProstDecoder {
            _pd: PhantomData,
            buffer_settings: BufferSettings::default(),
        }
    }
}

/// A [`Encoder`] that knows how to encode `T`.
#[derive(Debug, Clone, Default)]
pub struct ProstEncoder<T> {
    _pd: PhantomData<T>,
    buffer_settings: BufferSettings,
}

impl<T> ProstEncoder<T> {
    /// Get a new encoder with explicit buffer settings
    pub fn new(buffer_settings: BufferSettings) -> Self {
        Self {
            _pd: PhantomData,
            buffer_settings,
        }
    }
}

impl<T: Message> Encoder for ProstEncoder<T> {
    type Item = T;
    type Error = Status;

    type Encode = ReadyEncode<Status>;

    #[inline]
    fn encode(
        self: Pin<&mut Self>,
        item: Self::Item,
        mut buf: EncodeBuffer,
    ) -> Result<Self::Encode, Self::Error> {
        let _ = self;
        {
            let mut dst = buf.as_encode_buf();
            item.encode(&mut dst)
                .expect("Message only errors if not enough space");
        }

        Ok(ReadyEncode::new(buf))
    }

    #[inline]
    fn buffer_settings(&self) -> BufferSettings {
        self.buffer_settings
    }
}

/// A [`Decoder`] that knows how to decode `U`.
#[derive(Debug, Clone, Default)]
pub struct ProstDecoder<U> {
    _pd: PhantomData<U>,
    buffer_settings: BufferSettings,
}

impl<U> ProstDecoder<U> {
    /// Get a new decoder with explicit buffer settings
    pub fn new(buffer_settings: BufferSettings) -> Self {
        Self {
            _pd: PhantomData,
            buffer_settings,
        }
    }
}

impl<U: Message + Default + Send + 'static> Decoder for ProstDecoder<U> {
    type Item = U;
    type Error = Status;
    type Decode = Ready<Result<Option<U>, Status>>;

    fn decode(self: Pin<&mut Self>, buf: DecodeBuf<'_>) -> Result<Self::Decode, Self::Error> {
        let _ = self;
        let item = Message::decode(buf).map_err(from_decode_error)?;

        Ok(ready(Ok(Some(item))))
    }

    fn buffer_settings(&self) -> BufferSettings {
        self.buffer_settings
    }
}

fn from_decode_error(error: prost::DecodeError) -> Status {
    // Map Protobuf parse errors to an INTERNAL status code, as per
    // https://github.com/grpc/grpc/blob/master/doc/statuscodes.md
    Status::internal(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::{Buf, BufMut, BytesMut};
    use http_body::Body;
    use http_body_util::BodyExt as _;
    use std::{
        future::Future,
        pin::pin,
        task::{Context, Poll},
    };
    use tonic::codec::SingleMessageCompressionOverride;
    use tonic::codec::{AsyncEncode, EncodeBody, EncodeBuffer, HEADER_SIZE, Streaming};

    const LEN: usize = 10000;
    // The maximum uncompressed size in bytes for a message. Set to 2MB.
    const MAX_MESSAGE_SIZE: usize = 2 * 1024 * 1024;

    #[tokio::test]
    async fn decode() {
        let decoder = MockDecoder::default();

        let msg = vec![0u8; LEN];

        let mut buf = BytesMut::new();

        buf.reserve(msg.len() + HEADER_SIZE);
        buf.put_u8(0);
        buf.put_u32(msg.len() as u32);

        buf.put(&msg[..]);

        let body = body::MockBody::new(&buf[..], 10005, 0);

        let mut stream = Streaming::new_request(decoder, body, None, None);

        let mut i = 0usize;
        while let Some(output_msg) = stream.message().await.unwrap() {
            assert_eq!(output_msg.len(), msg.len());
            i += 1;
        }
        assert_eq!(i, 1);
    }

    #[tokio::test]
    async fn decode_waits_for_async_decoder() {
        let decoder = PendingDecoder::default();

        let msg = vec![0u8; LEN];

        let mut buf = BytesMut::new();

        buf.reserve(msg.len() + HEADER_SIZE);
        buf.put_u8(0);
        buf.put_u32(msg.len() as u32);

        buf.put(&msg[..]);

        let body = body::MockBody::new(&buf[..], 10005, 0);

        let mut stream = Streaming::new_request(decoder, body, None, None);

        let output_msg = stream
            .message()
            .await
            .expect("decode succeeds")
            .expect("message is present");
        assert_eq!(output_msg.len(), msg.len());
        assert!(stream.message().await.expect("stream ends").is_none());
    }

    #[tokio::test]
    async fn decode_async_decoder_returns_pending_before_message() {
        let decoder = PendingDecoder::default();

        let msg = vec![9u8; LEN];

        let mut buf = BytesMut::new();
        buf.reserve(msg.len() + HEADER_SIZE);
        buf.put_u8(0);
        buf.put_u32(msg.len() as u32);
        buf.put(&msg[..]);

        let body = body::MockBody::new(&buf[..], msg.len() + HEADER_SIZE, 0);
        let mut stream = Streaming::new_request(decoder, body, None, None);
        {
            let mut message = pin!(stream.message());

            let waker = std::task::Waker::noop();
            let mut cx = Context::from_waker(waker);

            assert!(matches!(message.as_mut().poll(&mut cx), Poll::Pending));

            let output_msg = match message.as_mut().poll(&mut cx) {
                Poll::Ready(Ok(Some(output_msg))) => output_msg,
                Poll::Ready(Ok(None)) => panic!("message stream ended"),
                Poll::Ready(Err(status)) => panic!("decode failed: {status}"),
                Poll::Pending => panic!("decode remained pending"),
            };

            assert_eq!(output_msg, msg);
        }
        assert!(stream.message().await.expect("stream ends").is_none());
    }

    #[tokio::test]
    async fn decode_max_message_size_exceeded() {
        let decoder = MockDecoder::default();

        let msg = vec![0u8; MAX_MESSAGE_SIZE + 1];

        let mut buf = BytesMut::new();

        buf.reserve(msg.len() + HEADER_SIZE);
        buf.put_u8(0);
        buf.put_u32(msg.len() as u32);

        buf.put(&msg[..]);

        let body = body::MockBody::new(&buf[..], MAX_MESSAGE_SIZE + HEADER_SIZE + 1, 0);

        let mut stream = Streaming::new_request(decoder, body, None, Some(MAX_MESSAGE_SIZE));

        let actual = stream.message().await.unwrap_err();

        let expected = Status::out_of_range(format!(
            "Error, decoded message length too large: found {} bytes, the limit is: {} bytes",
            msg.len(),
            MAX_MESSAGE_SIZE
        ));

        assert_eq!(actual.code(), expected.code());
        assert_eq!(actual.message(), expected.message());
    }

    #[tokio::test]
    async fn encode() {
        let encoder = MockEncoder::default();

        let msg = Vec::from(&[0u8; 1024][..]);

        let messages = std::iter::repeat_with(move || Ok::<_, Status>(msg.clone())).take(10000);
        let source = tokio_stream::iter(messages);

        let mut body = pin!(EncodeBody::new_server(
            encoder,
            source,
            None,
            SingleMessageCompressionOverride::default(),
            None,
        ));

        while let Some(r) = body.frame().await {
            r.unwrap();
        }
    }

    #[tokio::test]
    async fn encode_waits_for_async_encoder() {
        let encoder = PendingEncoder::default();
        let msg = Vec::from(&[7u8; 32][..]);

        let messages = std::iter::once(Ok::<_, Status>(msg.clone()));
        let source = tokio_stream::iter(messages);

        let mut body = pin!(EncodeBody::new_server(
            encoder,
            source,
            None,
            SingleMessageCompressionOverride::default(),
            None,
        ));

        let frame = body
            .frame()
            .await
            .expect("at least one frame")
            .expect("no error polling frame");
        let data = frame.into_data().expect("got data frame");

        assert_eq!(data[0], 0);
        assert_eq!(
            u32::from_be_bytes(data[1..HEADER_SIZE].try_into().unwrap()) as usize,
            msg.len()
        );
        assert_eq!(&data[HEADER_SIZE..], &msg[..]);

        let frame = body
            .frame()
            .await
            .expect("trailers frame")
            .expect("no error polling trailers");
        assert_eq!(
            frame
                .into_trailers()
                .expect("got trailers")
                .get(Status::GRPC_STATUS)
                .expect("grpc-status header"),
            "0"
        );
        assert!(body.is_end_stream());
    }

    #[tokio::test]
    async fn encode_async_encoder_returns_pending_before_data_frame() {
        let encoder = PendingEncoder::default();
        let msg = Vec::from(&[3u8; 32][..]);

        let messages = std::iter::once(Ok::<_, Status>(msg.clone()));
        let source = tokio_stream::iter(messages);

        let mut body = pin!(EncodeBody::new_server(
            encoder,
            source,
            None,
            SingleMessageCompressionOverride::default(),
            None,
        ));

        let waker = std::task::Waker::noop();
        let mut cx = Context::from_waker(waker);

        assert!(matches!(body.as_mut().poll_frame(&mut cx), Poll::Pending));

        let frame = match body.as_mut().poll_frame(&mut cx) {
            Poll::Ready(Some(Ok(frame))) => frame,
            Poll::Ready(Some(Err(status))) => panic!("encode failed: {status}"),
            Poll::Ready(None) => panic!("body ended"),
            Poll::Pending => panic!("encode remained pending"),
        };
        let data = frame.into_data().expect("got data frame");

        assert_eq!(data[0], 0);
        assert_eq!(
            u32::from_be_bytes(data[1..HEADER_SIZE].try_into().unwrap()) as usize,
            msg.len()
        );
        assert_eq!(&data[HEADER_SIZE..], &msg[..]);
    }

    #[tokio::test]
    async fn encode_max_message_size_exceeded() {
        let encoder = MockEncoder::default();

        let msg = vec![0u8; MAX_MESSAGE_SIZE + 1];

        let messages = std::iter::once(Ok::<_, Status>(msg));
        let source = tokio_stream::iter(messages);

        let mut body = pin!(EncodeBody::new_server(
            encoder,
            source,
            None,
            SingleMessageCompressionOverride::default(),
            Some(MAX_MESSAGE_SIZE),
        ));

        let frame = body
            .frame()
            .await
            .expect("at least one frame")
            .expect("no error polling frame");
        assert_eq!(
            frame
                .into_trailers()
                .expect("got trailers")
                .get(Status::GRPC_STATUS)
                .expect("grpc-status header"),
            "11"
        );
        assert!(body.is_end_stream());
    }

    // skip on windows because CI stumbles over our 4GB allocation
    #[cfg(not(target_family = "windows"))]
    #[tokio::test]
    async fn encode_too_big() {
        let encoder = MockEncoder::default();

        let msg = vec![0u8; u32::MAX as usize + 1];

        let messages = std::iter::once(Ok::<_, Status>(msg));
        let source = tokio_stream::iter(messages);

        let mut body = pin!(EncodeBody::new_server(
            encoder,
            source,
            None,
            SingleMessageCompressionOverride::default(),
            Some(usize::MAX),
        ));

        let frame = body
            .frame()
            .await
            .expect("at least one frame")
            .expect("no error polling frame");
        assert_eq!(
            frame
                .into_trailers()
                .expect("got trailers")
                .get(Status::GRPC_STATUS)
                .expect("grpc-status header"),
            "8"
        );
        assert!(body.is_end_stream());
    }

    #[derive(Debug, Clone, Default)]
    struct MockEncoder {}

    impl Encoder for MockEncoder {
        type Item = Vec<u8>;
        type Error = Status;
        type Encode = ReadyEncode<Status>;

        fn encode(
            self: Pin<&mut Self>,
            item: Self::Item,
            mut buf: EncodeBuffer,
        ) -> Result<Self::Encode, Self::Error> {
            let _ = self;
            buf.as_encode_buf().put(&item[..]);
            Ok(ReadyEncode::new(buf))
        }

        fn buffer_settings(&self) -> BufferSettings {
            Default::default()
        }
    }

    #[derive(Debug, Clone, Default)]
    struct PendingEncoder;

    #[derive(Debug)]
    struct PendingEncode {
        item: Vec<u8>,
        buf: Option<EncodeBuffer>,
        yielded: bool,
    }

    impl Encoder for PendingEncoder {
        type Item = Vec<u8>;
        type Error = Status;
        type Encode = PendingEncode;

        fn encode(
            self: Pin<&mut Self>,
            item: Self::Item,
            buf: EncodeBuffer,
        ) -> Result<Self::Encode, Self::Error> {
            let _ = self;
            Ok(PendingEncode {
                item,
                buf: Some(buf),
                yielded: false,
            })
        }
    }

    impl AsyncEncode for PendingEncode {
        type Error = Status;

        fn poll_encode(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<Result<EncodeBuffer, Self::Error>> {
            let this = self.get_mut();
            if !this.yielded {
                this.yielded = true;
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }

            let mut buf = this
                .buf
                .take()
                .expect("pending encode polled after completion");
            buf.as_encode_buf().put(&this.item[..]);
            Poll::Ready(Ok(buf))
        }
    }

    #[derive(Debug, Clone, Default)]
    struct MockDecoder {}

    impl Decoder for MockDecoder {
        type Item = Vec<u8>;
        type Error = Status;
        type Decode = Ready<Result<Option<Vec<u8>>, Status>>;

        fn decode(
            self: Pin<&mut Self>,
            mut buf: DecodeBuf<'_>,
        ) -> Result<Self::Decode, Self::Error> {
            let _ = self;
            let out = Vec::from(buf.chunk());
            buf.advance(LEN);
            Ok(ready(Ok(Some(out))))
        }

        fn buffer_settings(&self) -> BufferSettings {
            Default::default()
        }
    }

    #[derive(Debug, Clone, Default)]
    struct PendingDecoder;

    #[derive(Debug)]
    struct PendingDecode {
        item: Option<Vec<u8>>,
        yielded: bool,
    }

    impl Decoder for PendingDecoder {
        type Item = Vec<u8>;
        type Error = Status;
        type Decode = PendingDecode;

        fn decode(
            self: Pin<&mut Self>,
            mut buf: DecodeBuf<'_>,
        ) -> Result<Self::Decode, Self::Error> {
            let _ = self;
            let out = Vec::from(buf.chunk());
            buf.advance(LEN);
            Ok(PendingDecode {
                item: Some(out),
                yielded: false,
            })
        }
    }

    impl Future for PendingDecode {
        type Output = Result<Option<Vec<u8>>, Status>;

        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            let this = self.get_mut();
            if !this.yielded {
                this.yielded = true;
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }

            Poll::Ready(Ok(Some(
                this.item
                    .take()
                    .expect("pending decode polled after completion"),
            )))
        }
    }

    mod body {
        use bytes::Bytes;
        use http_body::{Body, Frame};
        use std::{
            pin::Pin,
            task::{Context, Poll},
        };
        use tonic::Status;

        #[derive(Debug)]
        pub(super) struct MockBody {
            data: Bytes,

            // the size of the partial message to send
            partial_len: usize,

            // the number of times we've sent
            count: usize,
        }

        impl MockBody {
            pub(super) fn new(b: &[u8], partial_len: usize, count: usize) -> Self {
                MockBody {
                    data: Bytes::copy_from_slice(b),
                    partial_len,
                    count,
                }
            }
        }

        impl Body for MockBody {
            type Data = Bytes;
            type Error = Status;

            fn poll_frame(
                mut self: Pin<&mut Self>,
                cx: &mut Context<'_>,
            ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
                // every other call to poll_data returns data
                let should_send = self.count % 2 == 0;
                let data_len = self.data.len();
                let partial_len = self.partial_len;
                let count = self.count;
                if data_len > 0 {
                    let result = if should_send {
                        let response =
                            self.data
                                .split_to(if count == 0 { partial_len } else { data_len });
                        Poll::Ready(Some(Ok(Frame::data(response))))
                    } else {
                        cx.waker().wake_by_ref();
                        Poll::Pending
                    };
                    // make some fake progress
                    self.count += 1;
                    result
                } else {
                    Poll::Ready(None)
                }
            }
        }
    }
}
