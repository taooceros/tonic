//! Generic encoding and decoding.
//!
//! This module contains the generic `Codec`, `Encoder` and `Decoder` traits.

mod buffer;
pub(crate) mod compression;
mod decode;
mod encode;
use crate::Status;
use std::{
    io,
    pin::Pin,
    task::{Context, Poll},
};

pub use self::buffer::{DecodeBuf, EncodeBuf, EncodeBuffer};
pub use self::compression::{CompressionEncoding, EnabledCompressionEncodings};
pub use self::decode::Streaming;
pub use self::encode::{EncodeBody, EncodedBytes};

// Doc hidden since this is used in a test in another crate, we can expose this publically later
// if we need it.
#[doc(hidden)]
pub use self::compression::SingleMessageCompressionOverride;

/// Unless overridden, this is the buffer size used for encoding requests.
/// This is spent per-rpc, so you may wish to adjust it. The default is
/// pretty good for most uses, but if you have a ton of concurrent rpcs
/// you may find it too expensive.
const DEFAULT_CODEC_BUFFER_SIZE: usize = 8 * 1024;
const DEFAULT_YIELD_THRESHOLD: usize = 32 * 1024;

/// Settings for how tonic allocates and grows buffers.
///
/// Tonic eagerly allocates the buffer_size per RPC, and grows
/// the buffer by buffer_size increments to handle larger messages.
/// Buffer size defaults to 8KiB.
///
/// Example:
/// ```ignore
/// Buffer start:       | 8kb |
/// Message received:   |   24612 bytes    |
/// Buffer grows:       | 8kb | 8kb | 8kb | 8kb |
/// ```
///
/// The buffer grows to the next largest buffer_size increment of
/// 32768 to hold 24612 bytes, which is just slightly too large for
/// the previous buffer increment of 24576.
///
/// If you use a smaller buffer size you will waste less memory, but
/// you will allocate more frequently. If one way or the other matters
/// more to you, you may wish to customize your tonic Codec (see
/// codec_buffers example).
///
/// Yield threshold is an optimization for streaming rpcs. Sometimes
/// you may have many small messages ready to send. When they are ready,
/// it is a much more efficient use of system resources to batch them
/// together into one larger send(). The yield threshold controls how
/// much you want to bulk up such a batch of ready-to-send messages.
/// The larger your yield threshold the more you will batch - and
/// consequently allocate contiguous memory, which might be relevant
/// if you're considering large numbers here.
/// If your server streaming rpc does not reach the yield threshold
/// before it reaches Poll::Pending (meaning, it's waiting for more
/// data from wherever you're streaming from) then Tonic will just send
/// along a smaller batch. Yield threshold is an upper-bound, it will
/// not affect the responsiveness of your streaming rpc (for reasonable
/// sizes of yield threshold).
/// Yield threshold defaults to 32 KiB.
#[derive(Clone, Copy, Debug)]
pub struct BufferSettings {
    buffer_size: usize,
    yield_threshold: usize,
}

impl BufferSettings {
    /// Create a new `BufferSettings`
    pub fn new(buffer_size: usize, yield_threshold: usize) -> Self {
        Self {
            buffer_size,
            yield_threshold,
        }
    }
}

impl Default for BufferSettings {
    fn default() -> Self {
        Self {
            buffer_size: DEFAULT_CODEC_BUFFER_SIZE,
            yield_threshold: DEFAULT_YIELD_THRESHOLD,
        }
    }
}

// Doc hidden because it's used in tests in another crate but not part of the
// public api.
#[doc(hidden)]
pub const HEADER_SIZE: usize =
    // compression flag
    std::mem::size_of::<u8>() +
    // data length
    std::mem::size_of::<u32>();

// The default maximum uncompressed size in bytes for a message. Defaults to 4MB.
const DEFAULT_MAX_RECV_MESSAGE_SIZE: usize = 4 * 1024 * 1024;
const DEFAULT_MAX_SEND_MESSAGE_SIZE: usize = usize::MAX;

/// Trait that knows how to encode and decode gRPC messages.
pub trait Codec {
    /// The encodable message.
    type Encode: Send + 'static;
    /// The decodable message.
    type Decode: Send + 'static;

    /// The encoder that can encode a message.
    type Encoder: Encoder<Item = Self::Encode, Error = Status> + Send + 'static;
    /// The decoder that can decode a message.
    type Decoder: Decoder<Item = Self::Decode, Error = Status> + Send + 'static;

    /// Fetch the encoder.
    fn encoder(&mut self) -> Self::Encoder;
    /// Fetch the decoder.
    fn decoder(&mut self) -> Self::Decoder;
}

/// Encodes gRPC message types
pub trait Encoder {
    /// The type that is encoded.
    type Item;

    /// The type of encoding errors.
    ///
    /// The type of unrecoverable frame encoding errors.
    type Error: From<io::Error>;

    #[doc(hidden)]
    const ENCODE_READY: bool = false;

    /// Encodes one message immediately.
    ///
    /// Implementations that set [`Self::ENCODE_READY`] to `true` must override
    /// this method and complete without yielding.
    #[inline]
    #[doc(hidden)]
    fn encode_ready(
        self: Pin<&mut Self>,
        item: Self::Item,
        _dst: EncodeBuf<'_>,
    ) -> Result<(), Self::Error> {
        let _ = item;
        unreachable!("ENCODE_READY encoders must override encode_ready")
    }

    /// Starts encoding one message into owned encode storage.
    ///
    /// Implementations that return before completion must keep all in-flight
    /// item, buffer, and cancellation state inside `self` until
    /// [`Self::poll_encode`] returns the completed buffer.
    #[doc(hidden)]
    fn start_encode(
        self: Pin<&mut Self>,
        item: Self::Item,
        dst: EncodeBuffer,
    ) -> Result<(), Self::Error> {
        let _ = item;
        let _ = dst;
        unreachable!("async encoders must override start_encode")
    }

    /// Polls encoding of the message most recently passed to
    /// [`Self::start_encode`].
    ///
    /// This mirrors [`http_body::Body::poll_frame`]: the encoder owns all
    /// in-flight state, and polling takes only pinned encoder state plus the
    /// task context.
    #[doc(hidden)]
    fn poll_encode(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<EncodeBuffer, Self::Error>> {
        let _ = self;
        let _ = cx;
        unreachable!("async encoders must override poll_encode")
    }

    /// Controls how tonic creates and expands encode buffers.
    fn buffer_settings(&self) -> BufferSettings {
        BufferSettings::default()
    }
}

/// Decodes gRPC message types
pub trait Decoder {
    /// The type that is decoded.
    type Item;

    /// The type of unrecoverable frame decoding errors.
    type Error: From<io::Error>;

    /// Polls decoding of one full message from the provided buffer.
    ///
    /// The buffer contains exactly the bytes of a full message. Implementations
    /// must not retain or advance the buffer after returning [`Poll::Pending`];
    /// tonic will pass a fresh view of the same message bytes on the next poll.
    fn poll_decode(
        &mut self,
        cx: &mut Context<'_>,
        src: DecodeBuf<'_>,
    ) -> Poll<Result<Option<Self::Item>, Self::Error>>;

    /// Controls how tonic creates and expands decode buffers.
    fn buffer_settings(&self) -> BufferSettings {
        BufferSettings::default()
    }
}
