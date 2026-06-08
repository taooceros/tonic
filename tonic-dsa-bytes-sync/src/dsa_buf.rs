//! Synchronous DSA raw-buffer codec for tonic.
//!
//! This module mirrors tonic's internal raw `Buf`/`Bytes` codec shape while
//! keeping DSA device details inside this experimental crate. Encoding accepts a
//! caller-provided [`bytes::Buf`] and copies the gRPC message payload into
//! tonic's encode buffer with synchronous DSA memmove descriptors when a work
//! queue is configured.

use super::{
    DsaWorkQueue, SharedDsaWorkQueue, ensure_dsa_success, poll_dsa_descriptor_to_completion,
    process_dsa_work_queue,
};
use bytes::{Buf, BufMut, Bytes};
use idxd_rust::DsaHwDesc;
use std::fmt;
use std::task::{Context, Poll};
use tonic::Status;
use tonic::codec::{BufferSettings, Codec, DecodeBuf, Decoder, EncodeBuf, Encoder};

/// A tonic codec that sends raw [`Buf`] payloads and receives raw [`Bytes`].
///
/// This codec still uses tonic's normal gRPC message framing and HTTP/2 body
/// path. It treats the caller-provided buffer as the complete message payload
/// and, when configured, uses synchronous DSA memmove descriptors for the encode
/// copy from the input buffer into tonic's output buffer.
#[derive(Debug, Clone)]
pub struct DsaBufCodec {
    work_queue: Option<SharedDsaWorkQueue>,
}

impl DsaBufCodec {
    /// Creates a raw-buffer codec using the process DSA work queue when configured.
    pub fn new() -> Self {
        Self::with_optional_work_queue(process_dsa_work_queue())
    }

    /// Creates a raw-buffer codec with an explicit shared DSA work queue.
    pub fn with_work_queue(work_queue: SharedDsaWorkQueue) -> Self {
        Self::with_optional_work_queue(Some(work_queue))
    }

    /// Creates a raw-buffer codec that keeps the DSA type but encodes on the CPU path.
    pub fn without_work_queue() -> Self {
        Self::with_optional_work_queue(None)
    }

    fn with_optional_work_queue(work_queue: Option<SharedDsaWorkQueue>) -> Self {
        Self { work_queue }
    }

    /// Builds a raw synchronous DSA buffer encoder with explicit tonic buffer settings.
    pub fn raw_encoder(buffer_settings: BufferSettings) -> DsaSyncBufEncoder {
        DsaSyncBufEncoder::new(buffer_settings)
    }

    /// Builds a raw synchronous DSA buffer encoder with an explicit shared work queue.
    pub fn raw_encoder_with_work_queue(
        buffer_settings: BufferSettings,
        work_queue: SharedDsaWorkQueue,
    ) -> DsaSyncBufEncoder {
        DsaSyncBufEncoder::with_work_queue(buffer_settings, work_queue)
    }

    /// Builds a raw bytes decoder with explicit tonic buffer settings.
    pub fn raw_decoder(buffer_settings: BufferSettings) -> DsaSyncBytesDecoder {
        DsaSyncBytesDecoder::new(buffer_settings)
    }
}

impl Default for DsaBufCodec {
    fn default() -> Self {
        Self::new()
    }
}

impl Codec for DsaBufCodec {
    type Encode = Box<dyn Buf + Send + Sync>;
    type Decode = Bytes;

    type Encoder = DsaSyncBufEncoder;
    type Decoder = DsaSyncBytesDecoder;

    fn encoder(&mut self) -> Self::Encoder {
        DsaSyncBufEncoder::with_optional_work_queue(
            BufferSettings::default(),
            self.work_queue.clone(),
        )
    }

    fn decoder(&mut self) -> Self::Decoder {
        DsaSyncBytesDecoder::new(BufferSettings::default())
    }
}

/// A raw-buffer encoder that can synchronously copy payload bytes with DSA.
pub struct DsaSyncBufEncoder {
    buffer_settings: BufferSettings,
    work_queue: Option<SharedDsaWorkQueue>,
}

impl fmt::Debug for DsaSyncBufEncoder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DsaSyncBufEncoder")
            .field("buffer_settings", &self.buffer_settings)
            .field("work_queue", &self.work_queue)
            .finish()
    }
}

impl Clone for DsaSyncBufEncoder {
    fn clone(&self) -> Self {
        Self::with_optional_work_queue(self.buffer_settings, self.work_queue.clone())
    }
}

impl Default for DsaSyncBufEncoder {
    fn default() -> Self {
        Self::new(BufferSettings::default())
    }
}

impl DsaSyncBufEncoder {
    /// Gets a new raw-buffer encoder using the process work queue when configured.
    pub fn new(buffer_settings: BufferSettings) -> Self {
        Self::with_optional_work_queue(buffer_settings, process_dsa_work_queue())
    }

    /// Gets a new raw-buffer encoder with an explicit shared work queue.
    pub fn with_work_queue(
        buffer_settings: BufferSettings,
        work_queue: SharedDsaWorkQueue,
    ) -> Self {
        Self::with_optional_work_queue(buffer_settings, Some(work_queue))
    }

    /// Gets an encoder that keeps the DSA sync type but encodes on the CPU path.
    pub fn without_work_queue(buffer_settings: BufferSettings) -> Self {
        Self::with_optional_work_queue(buffer_settings, None)
    }

    fn with_optional_work_queue(
        buffer_settings: BufferSettings,
        work_queue: Option<SharedDsaWorkQueue>,
    ) -> Self {
        Self {
            buffer_settings,
            work_queue,
        }
    }

    #[inline]
    fn encode_item(
        &mut self,
        mut item: Box<dyn Buf + Send + Sync>,
        mut dst: EncodeBuf<'_>,
    ) -> Result<(), Status> {
        let payload_len = item.remaining();
        if self
            .work_queue
            .as_ref()
            .is_some_and(|work_queue| work_queue.accelerates(payload_len))
        {
            return self.encode_item_with_dsa(&mut item, payload_len, &mut dst);
        }

        dst.put(&mut *item);
        Ok(())
    }

    fn encode_item_with_dsa(
        &mut self,
        item: &mut dyn Buf,
        payload_len: usize,
        dst: &mut EncodeBuf<'_>,
    ) -> Result<(), Status> {
        if payload_len > u32::MAX as usize {
            return Err(Status::internal(format!(
                "dsa buf encode cannot copy {payload_len} bytes; maximum DSA transfer is {} bytes",
                u32::MAX
            )));
        }

        let work_queue = self
            .work_queue
            .as_ref()
            .expect("DSA work queue checked before encoding");

        // SAFETY: `dst` reserves stable spare capacity for `payload_len` bytes.
        // The closure submits synchronous DSA memmove descriptors for all source
        // chunks and returns `Ok(())` only after every descriptor completes
        // successfully, so tonic advances the readable length only after all
        // bytes are initialized.
        unsafe {
            dst.put_uninit_slice_with(payload_len, |dst| {
                copy_buf_to_uninit_with_dsa(item, dst, payload_len, work_queue.as_ref())
            })
        }
    }
}

impl Encoder for DsaSyncBufEncoder {
    type Item = Box<dyn Buf + Send + Sync>;
    type Error = Status;

    const ENCODE_READY: bool = true;

    #[inline]
    fn encode_ready(&mut self, item: Self::Item, dst: EncodeBuf<'_>) -> Result<(), Self::Error> {
        self.encode_item(item, dst)
    }

    #[inline]
    fn poll_encode(
        &mut self,
        _cx: &mut Context<'_>,
        item: &mut Option<Self::Item>,
        dst: EncodeBuf<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        let item = item.take().expect("encoder item available");
        Poll::Ready(self.encode_item(item, dst))
    }

    #[inline]
    fn buffer_settings(&self) -> BufferSettings {
        self.buffer_settings
    }
}

/// A raw bytes decoder for [`DsaBufCodec`].
#[derive(Debug, Clone)]
pub struct DsaSyncBytesDecoder {
    buffer_settings: BufferSettings,
}

impl DsaSyncBytesDecoder {
    /// Gets a new raw bytes decoder with explicit tonic buffer settings.
    pub fn new(buffer_settings: BufferSettings) -> Self {
        Self { buffer_settings }
    }
}

impl Default for DsaSyncBytesDecoder {
    fn default() -> Self {
        Self::new(BufferSettings::default())
    }
}

impl Decoder for DsaSyncBytesDecoder {
    type Item = Bytes;
    type Error = Status;

    #[inline]
    fn poll_decode(
        &mut self,
        _cx: &mut Context<'_>,
        mut src: DecodeBuf<'_>,
    ) -> Poll<Result<Option<Self::Item>, Self::Error>> {
        Poll::Ready(Ok(Some(src.copy_to_bytes(src.remaining()))))
    }

    #[inline]
    fn buffer_settings(&self) -> BufferSettings {
        self.buffer_settings
    }
}

const PAGE_SIZE: usize = 4096;

fn copy_buf_to_uninit_with_dsa(
    src: &mut dyn Buf,
    mut dst: *mut u8,
    expected_len: usize,
    work_queue: &DsaWorkQueue,
) -> Result<(), Status> {
    let mut remaining = expected_len;
    while remaining != 0 {
        let chunk = src.chunk();
        if chunk.is_empty() {
            return Err(Status::internal(format!(
                "dsa buf encode source ended with {remaining} bytes remaining"
            )));
        }

        let chunk_len = chunk.len().min(remaining);
        copy_slice_to_uninit_with_dsa(chunk.as_ptr(), dst, chunk_len, work_queue)?;

        src.advance(chunk_len);
        remaining -= chunk_len;
        // SAFETY: `dst` points into the `expected_len` byte spare region and is
        // advanced only by the number of bytes just initialized.
        unsafe {
            dst = dst.add(chunk_len);
        }
    }

    if src.has_remaining() {
        return Err(Status::internal(format!(
            "dsa buf encode copied {expected_len} bytes but source still has {} bytes",
            src.remaining()
        )));
    }

    Ok(())
}

fn copy_slice_to_uninit_with_dsa(
    src: *const u8,
    dst: *mut u8,
    len: usize,
    work_queue: &DsaWorkQueue,
) -> Result<(), Status> {
    debug_assert!(len != 0);

    touch_pages_for_dsa(src, dst, len);

    let mut desc = DsaHwDesc::default();
    desc.fill_memmove(src, dst, len as u32);

    let completion = poll_dsa_descriptor_to_completion(&work_queue.engine, desc);
    ensure_dsa_success(completion, len, &work_queue.config.device_path)
}

fn touch_pages_for_dsa(src: *const u8, dst: *mut u8, len: usize) {
    debug_assert!(len != 0);

    let mut offset = 0;
    while offset < len {
        // SAFETY: `offset < len`, so both addresses are in the caller-provided
        // source and destination ranges. The destination byte is deliberately
        // initialized before the hardware copy so the DSA descriptor does not
        // fail on a not-yet-resident output page.
        unsafe {
            std::ptr::read_volatile(src.add(offset));
            std::ptr::write_volatile(dst.add(offset), 0);
        }
        offset = offset.saturating_add(PAGE_SIZE);
    }

    let last = len - 1;
    // SAFETY: `last < len`; touch the final byte when the range does not end on
    // a page boundary.
    unsafe {
        std::ptr::read_volatile(src.add(last));
        std::ptr::write_volatile(dst.add(last), 0);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http_body::Body;
    use std::pin::pin;
    use tonic::codec::{EncodeBody, HEADER_SIZE};

    #[test]
    fn codec_builds_raw_encoder_and_decoder_without_process_work_queue() {
        let mut codec = DsaBufCodec::without_work_queue();
        let _encoder = codec.encoder();
        let _decoder = codec.decoder();
    }

    #[test]
    fn encoder_without_work_queue_uses_cpu_path() {
        let payload = Bytes::from_static(b"raw bytes payload");
        let expected_payload = payload.clone();
        let source = tokio_stream::iter(std::iter::once(Ok::<_, Status>(
            Box::new(payload) as Box<dyn Buf + Send + Sync>
        )));
        let mut body = pin!(EncodeBody::new_client(
            DsaSyncBufEncoder::without_work_queue(BufferSettings::default()),
            source,
            None,
            None,
        ));
        let waker = std::task::Waker::noop();
        let mut cx = Context::from_waker(waker);

        let frame = match body.as_mut().poll_frame(&mut cx) {
            Poll::Ready(Some(Ok(frame))) => frame,
            Poll::Ready(Some(Err(status))) => panic!("encode failed: {status}"),
            Poll::Ready(None) => panic!("body ended before data"),
            Poll::Pending => panic!("CPU encode should not be pending"),
        };
        let data = frame.into_data().expect("got data frame");

        assert_eq!(data[0], 0);
        assert_eq!(
            u32::from_be_bytes(data[1..HEADER_SIZE].try_into().unwrap()) as usize,
            expected_payload.len()
        );
        assert_eq!(&data[HEADER_SIZE..], &expected_payload[..]);
    }
}
