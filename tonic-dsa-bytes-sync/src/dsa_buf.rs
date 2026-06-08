//! Synchronous DSA raw-buffer codec for tonic.
//!
//! This module mirrors tonic's internal raw `Buf`/`Bytes` codec shape while
//! keeping DSA device details inside this experimental crate. Encoding accepts a
//! caller-provided [`bytes::Buf`] and copies the gRPC message payload into
//! tonic's encode buffer with synchronous DSA memmove descriptors when a work
//! queue is configured.

use super::{
    DsaWorkQueue, SharedDsaWorkQueue, poll_dsa_descriptor_to_completion, process_dsa_work_queue,
};
use bytes::{Buf, BufMut, Bytes};
use idxd_rust::{DsaCompletionRecord, DsaCompletionStatus, DsaHwDesc};
use std::path::Path;
use std::task::{Context, Poll};
use std::{fmt, pin::Pin};
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
    fn encode_ready(
        self: Pin<&mut Self>,
        item: Self::Item,
        dst: EncodeBuf<'_>,
    ) -> Result<(), Self::Error> {
        self.get_mut().encode_item(item, dst)
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
const DSA_COMP_STATUS_WRITE: u8 = 0x80;
const MAX_NO_PROGRESS_PAGE_FAULT_RETRIES: usize = 4;

#[derive(Debug)]
struct DsaMemmoveRetry {
    src: *const u8,
    dst: *mut u8,
    remaining: usize,
    original_len: usize,
    retry_count: usize,
    max_retries: usize,
    no_progress_retry_count: usize,
}

impl DsaMemmoveRetry {
    fn new(src: *const u8, dst: *mut u8, len: usize) -> Self {
        Self {
            src,
            dst,
            remaining: len,
            original_len: len,
            retry_count: 0,
            max_retries: len.div_ceil(PAGE_SIZE).saturating_mul(2).saturating_add(4),
            no_progress_retry_count: 0,
        }
    }

    fn fill_desc(&self, desc: &mut DsaHwDesc) -> Result<(), Status> {
        if self.remaining > u32::MAX as usize {
            return Err(Status::internal(format!(
                "dsa bytes encode cannot copy remaining {} bytes; maximum DSA transfer is {} bytes",
                self.remaining,
                u32::MAX
            )));
        }

        desc.fill_memmove(self.src, self.dst, self.remaining as u32);
        Ok(())
    }

    fn handle_completion(
        &mut self,
        completion: DsaCompletionRecord,
        device_path: &Path,
    ) -> Result<DsaRetryAction, Status> {
        let raw_status = completion.status();
        let status = DsaCompletionStatus::mask(raw_status);
        if status == DsaCompletionStatus::Success.as_u8() {
            return Ok(DsaRetryAction::Complete);
        }

        if status != DsaCompletionStatus::PageFaultNoBof.as_u8() {
            return Err(dsa_completion_error(
                "dsa bytes encode failed",
                completion,
                self.original_len,
                self.remaining,
                device_path,
            ));
        }

        self.handle_page_fault(
            raw_status,
            completion.bytes_completed(),
            completion.fault_addr(),
            device_path,
        )?;
        Ok(DsaRetryAction::Retry)
    }

    fn handle_page_fault(
        &mut self,
        raw_status: u8,
        bytes_completed: u32,
        fault_addr: u64,
        device_path: &Path,
    ) -> Result<(), Status> {
        if self.retry_count >= self.max_retries {
            return Err(Status::internal(format!(
                "dsa bytes encode page-fault retry limit exceeded on {} for {} bytes: retries={} remaining={} fault_addr={fault_addr:#x}",
                device_path.display(),
                self.original_len,
                self.retry_count,
                self.remaining
            )));
        }

        let completed = bytes_completed as usize;
        if completed > self.remaining {
            return Err(Status::internal(format!(
                "dsa bytes encode page fault on {} reported bytes_completed={} beyond remaining {} for {} byte transfer",
                device_path.display(),
                completed,
                self.remaining,
                self.original_len
            )));
        }

        if completed == 0 {
            self.no_progress_retry_count += 1;
            if self.no_progress_retry_count > MAX_NO_PROGRESS_PAGE_FAULT_RETRIES {
                return Err(Status::internal(format!(
                    "dsa bytes encode made no progress after {} page-fault retries on {} for {} bytes: remaining={} fault_addr={fault_addr:#x}",
                    self.no_progress_retry_count,
                    device_path.display(),
                    self.original_len,
                    self.remaining
                )));
            }
        } else {
            self.no_progress_retry_count = 0;
        }

        touch_fault_addr(raw_status, fault_addr)?;

        if completed == self.remaining {
            return Err(Status::internal(format!(
                "dsa bytes encode page fault on {} left no remaining bytes to retry for {} byte transfer",
                device_path.display(),
                self.original_len
            )));
        }

        // SAFETY: `completed <= self.remaining`, and both pointers describe the
        // current still-owned source/destination ranges for this memmove.
        unsafe {
            self.src = self.src.add(completed);
            self.dst = self.dst.add(completed);
        }
        self.remaining -= completed;
        self.retry_count += 1;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DsaRetryAction {
    Complete,
    Retry,
}

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

    let mut retry = DsaMemmoveRetry::new(src, dst, len);
    loop {
        let mut desc = DsaHwDesc::default();
        retry.fill_desc(&mut desc)?;
        let completion = poll_dsa_descriptor_to_completion(&work_queue.engine, desc);
        match retry.handle_completion(completion, &work_queue.config.device_path)? {
            DsaRetryAction::Complete => return Ok(()),
            DsaRetryAction::Retry => {}
        }
    }
}

fn dsa_completion_error(
    context: &str,
    completion: DsaCompletionRecord,
    encoded_len: usize,
    remaining: usize,
    device_path: &Path,
) -> Status {
    let raw_status = completion.status();
    Status::internal(format!(
        "{context} on {} for {encoded_len} bytes: status={raw_status:#04x} result={:#04x} bytes_completed={} fault_addr={:#x} remaining={remaining}",
        device_path.display(),
        completion.result(),
        completion.bytes_completed(),
        completion.fault_addr()
    ))
}

fn touch_fault_addr(raw_status: u8, fault_addr: u64) -> Result<(), Status> {
    if fault_addr == 0 {
        return Err(Status::internal(
            "dsa bytes encode page fault reported a null fault address",
        ));
    }

    let ptr = fault_addr as *mut u8;
    if raw_status & DSA_COMP_STATUS_WRITE != 0 {
        // SAFETY: DSA reports `fault_addr` as a process virtual address that
        // faulted on a write. The destination may be tonic's uninitialized spare
        // capacity, so write a byte to fault the page in without first reading
        // an uninitialized value. DSA will overwrite the byte when the adjusted
        // descriptor is resubmitted.
        unsafe {
            std::ptr::write_volatile(ptr, 0);
        }
    } else {
        // SAFETY: DSA reports `fault_addr` as a process virtual address that
        // faulted on a read. A volatile read is enough to fault the page in.
        unsafe {
            let _ = std::ptr::read_volatile(ptr.cast_const());
        }
    }

    Ok(())
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
    use std::path::Path;
    use std::pin::pin;
    use tonic::codec::{EncodeBody, HEADER_SIZE};

    #[test]
    fn page_fault_retry_advances_memmove_descriptor() {
        let src = [0x5a; 512];
        let mut dst = [0u8; 512];
        let mut retry = DsaMemmoveRetry::new(src.as_ptr(), dst.as_mut_ptr(), src.len());
        let fault_addr = dst.as_mut_ptr() as u64;

        retry
            .handle_page_fault(
                DSA_COMP_STATUS_WRITE | DsaCompletionStatus::PageFaultNoBof.as_u8(),
                128,
                fault_addr,
                Path::new("/dev/dsa/test"),
            )
            .expect("page fault adjusted for retry");

        let mut desc = DsaHwDesc::default();
        retry.fill_desc(&mut desc).expect("descriptor filled");

        assert_eq!(desc.src_addr(), src.as_ptr().wrapping_add(128) as u64);
        assert_eq!(desc.dst_addr(), dst.as_mut_ptr().wrapping_add(128) as u64);
        assert_eq!(desc.xfer_size(), 384);
        assert_eq!(dst[0], 0);
    }

    #[test]
    fn page_fault_retry_rejects_repeated_no_progress() {
        let src = [0x5a; 64];
        let mut dst = [0u8; 64];
        let mut retry = DsaMemmoveRetry::new(src.as_ptr(), dst.as_mut_ptr(), src.len());
        let fault_addr = src.as_ptr() as u64;

        for _ in 0..MAX_NO_PROGRESS_PAGE_FAULT_RETRIES {
            retry
                .handle_page_fault(
                    DsaCompletionStatus::PageFaultNoBof.as_u8(),
                    0,
                    fault_addr,
                    Path::new("/dev/dsa/test"),
                )
                .expect("bounded no-progress retry allowed");
        }

        let err = retry
            .handle_page_fault(
                DsaCompletionStatus::PageFaultNoBof.as_u8(),
                0,
                fault_addr,
                Path::new("/dev/dsa/test"),
            )
            .expect_err("repeated no-progress retry rejected");
        assert!(err.to_string().contains("made no progress"));
    }

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
