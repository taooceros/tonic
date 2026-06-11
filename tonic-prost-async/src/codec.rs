use prost::{
    Message,
    transfer::{AsyncEncodeExt, CpuEncode, CpuEngine, EncodeDst, EncodeOptions},
};
use std::{
    future::{Future, Ready, ready},
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};
use tonic::Status;
use tonic::codec::{BufferSettings, Codec, DecodeBuf, Decoder, EncodeBuf, EncodeBuffer, Encoder};

/// A [`Codec`] that implements `application/grpc+proto` via prost's owned async encode API.
#[derive(Debug, Clone)]
pub struct ProstCodec<T, U> {
    _pd: PhantomData<(T, U)>,
}

impl<T, U> ProstCodec<T, U> {
    /// Configure a ProstCodec with encoder/decoder buffer settings.
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
    pub fn raw_encoder(buffer_settings: BufferSettings) -> <Self as Codec>::Encoder {
        ProstEncoder {
            _pd: PhantomData,
            buffer_settings,
        }
    }

    /// A tool for building custom codecs based on prost encoding and decoding.
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

/// A [`Encoder`] that encodes `T` by returning prost's owned CPU encode future.
#[derive(Debug, Clone, Default)]
pub struct ProstEncoder<T> {
    _pd: PhantomData<T>,
    buffer_settings: BufferSettings,
}

#[derive(Debug)]
struct ProstEncodeDst(EncodeBuffer);

impl ProstEncodeDst {
    #[inline]
    fn into_inner(self) -> EncodeBuffer {
        self.0
    }
}

impl EncodeDst for ProstEncodeDst {
    type BufMut<'a> = EncodeBuf<'a>;

    #[inline]
    fn as_buf_mut(&mut self) -> Self::BufMut<'_> {
        self.0.as_encode_buf()
    }
}

/// Owned future returned by [`ProstEncoder`] for one message encode.
#[derive(Debug)]
pub struct ProstEncode {
    inner: CpuEncode<ProstEncodeDst>,
}

impl Future for ProstEncode {
    type Output = Result<EncodeBuffer, Status>;

    #[inline]
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match Pin::new(&mut self.inner).poll(cx) {
            Poll::Ready(Ok(dst)) => Poll::Ready(Ok(dst.into_inner())),
            Poll::Ready(Err(_error)) => {
                panic!("Message only errors if not enough space")
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<T> ProstEncoder<T> {
    /// Get a new encoder with explicit buffer settings.
    pub fn new(buffer_settings: BufferSettings) -> Self {
        Self {
            _pd: PhantomData,
            buffer_settings,
        }
    }
}

impl<T: Message + Send + 'static> Encoder for ProstEncoder<T> {
    type Item = T;
    type Error = Status;

    type Encode = ProstEncode;

    #[inline]
    fn encode(
        self: Pin<&mut Self>,
        item: Self::Item,
        buf: EncodeBuffer,
    ) -> Result<Self::Encode, Self::Error> {
        let _ = self;
        let inner = item
            .encode_async(CpuEngine, ProstEncodeDst(buf), EncodeOptions::default())
            .expect("CpuEngine starts encoding");

        Ok(ProstEncode { inner })
    }

    #[inline]
    fn buffer_settings(&self) -> BufferSettings {
        self.buffer_settings
    }
}

/// A [`Decoder`] that knows how to decode `U` with prost.
#[derive(Debug, Clone, Default)]
pub struct ProstDecoder<U> {
    _pd: PhantomData<U>,
    buffer_settings: BufferSettings,
}

impl<U> ProstDecoder<U> {
    /// Get a new decoder with explicit buffer settings.
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
    use http_body::Body;
    use http_body_util::BodyExt as _;
    use std::task::Poll;
    use tonic::codec::SingleMessageCompressionOverride;
    use tonic::codec::{EncodeBody, HEADER_SIZE, Streaming};

    #[derive(Clone, PartialEq, ::prost::Message)]
    struct RoundTripMessage {
        #[prost(string, tag = "1")]
        name: String,
        #[prost(uint32, tag = "2")]
        id: u32,
    }

    #[test]
    fn prost_encoder_uses_owned_async_future_type() {
        assert_send_static::<<ProstEncoder<RoundTripMessage> as Encoder>::Encode>();
    }

    #[tokio::test]
    async fn prost_encoder_round_trips_with_prost_decoder() {
        let msg = RoundTripMessage {
            name: "owned async encode".to_string(),
            id: 42,
        };
        let messages = std::iter::once(Ok::<_, Status>(msg.clone()));
        let source = tokio_stream::iter(messages);

        let mut body = std::pin::pin!(EncodeBody::new_server(
            ProstEncoder::<RoundTripMessage>::default(),
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
            data.len() - HEADER_SIZE
        );

        let body = body::MockBody::new(&data[..], data.len(), 0);
        let mut stream = Streaming::new_request(
            ProstDecoder::<RoundTripMessage>::default(),
            body,
            None,
            None,
        );

        let decoded = stream
            .message()
            .await
            .expect("decode succeeds")
            .expect("message is present");
        assert_eq!(decoded, msg);
        assert!(stream.message().await.expect("stream ends").is_none());
    }

    #[tokio::test]
    async fn async_cpu_encoder_waits_for_tonic_to_poll_encode_future() {
        let encoder = YieldingCpuEncoder::default();
        let msg = RoundTripMessage {
            name: "poll boundary".to_string(),
            id: 7,
        };
        let messages = std::iter::once(Ok::<_, Status>(msg));
        let source = tokio_stream::iter(messages);

        let mut body = std::pin::pin!(EncodeBody::new_server(
            encoder,
            source,
            None,
            SingleMessageCompressionOverride::default(),
            None,
        ));

        let waker = std::task::Waker::noop();
        let mut cx = std::task::Context::from_waker(waker);

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
            data.len() - HEADER_SIZE
        );
    }

    fn assert_send_static<T: Send + 'static>() {}

    #[derive(Debug, Clone)]
    struct YieldingCpuEncoder<T = RoundTripMessage> {
        _pd: std::marker::PhantomData<T>,
    }

    impl<T> Default for YieldingCpuEncoder<T> {
        fn default() -> Self {
            Self {
                _pd: std::marker::PhantomData,
            }
        }
    }

    impl<T: Message + Send + 'static> Encoder for YieldingCpuEncoder<T> {
        type Item = T;
        type Error = Status;
        type Encode = YieldingCpuEncode;

        fn encode(
            self: Pin<&mut Self>,
            item: Self::Item,
            buf: EncodeBuffer,
        ) -> Result<Self::Encode, Self::Error> {
            let _ = self;
            let inner = item
                .encode_async(CpuEngine, ProstEncodeDst(buf), EncodeOptions::default())
                .expect("CpuEngine starts encoding");
            Ok(YieldingCpuEncode {
                yielded: false,
                inner: Some(ProstEncode { inner }),
            })
        }
    }

    #[derive(Debug)]
    struct YieldingCpuEncode {
        yielded: bool,
        inner: Option<ProstEncode>,
    }

    impl Future for YieldingCpuEncode {
        type Output = Result<EncodeBuffer, Status>;

        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            if !self.yielded {
                self.yielded = true;
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }

            let mut inner = self
                .inner
                .take()
                .expect("yielding encode polled after completion");
            Pin::new(&mut inner).poll(cx)
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
            partial_len: usize,
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
                    self.count += 1;
                    result
                } else {
                    Poll::Ready(None)
                }
            }
        }
    }
}
