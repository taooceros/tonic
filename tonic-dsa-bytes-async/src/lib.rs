//! Experimental asynchronous DSA bytes codec for tonic.
//!
//! This crate exercises tonic's `Encoder::poll_encode` path with an explicit
//! in-flight Intel DSA copy. The encoder keeps ordinary CPU encoding as the
//! default when no work queue is configured. When DSA is enabled, it submits a
//! memmove descriptor, returns `Poll::Pending`, and finishes the gRPC frame after
//! the descriptor completion record reaches a terminal status.
//!
//! The implementation reserves final `EncodeBuf` storage, assumes that storage
//! remains stable while `poll_encode` returns `Poll::Pending`, and commits the
//! initialized bytes after the DSA completion record reaches a terminal status.

#![warn(
    missing_debug_implementations,
    missing_docs,
    rust_2018_idioms,
    unreachable_pub
)]
#![doc(
    html_logo_url = "https://raw.githubusercontent.com/tokio-rs/website/master/public/img/icons/tonic.svg"
)]
#![doc(issue_tracker_base_url = "https://github.com/hyperium/tonic/issues/")]

use bytes::{Buf, BufMut, Bytes};
use idxd_rust::{DsaCompletionRecord, DsaCompletionStatus, DsaHwDesc, WqPortal, detect_wq_mode};
use std::{
    fmt,
    marker::PhantomPinned,
    path::{Path, PathBuf},
    pin::Pin,
    sync::{Arc, LazyLock, Mutex},
    task::{Context, Poll},
};
use tonic::Status;
use tonic::codec::{BufferSettings, Codec, DecodeBuf, Decoder, EncodeBuf, Encoder};

const DEFAULT_DSA_MIN_MESSAGE_BYTES: usize = 1;
const PAGE_SIZE: usize = 4096;

static PROCESS_DSA_WORK_QUEUE: LazyLock<Mutex<Option<SharedDsaWorkQueue>>> =
    LazyLock::new(|| Mutex::new(None));

/// Shared DSA work queue handle used by async DSA bytes encoders.
pub type SharedDsaWorkQueue = Arc<DsaWorkQueue>;

/// Process-wide DSA settings for the experimental bytes encode copy lane.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DsaConfig {
    /// IDXD work-queue device, for example `/dev/dsa/wq0.0`.
    pub device_path: PathBuf,
    /// Minimum gRPC message payload size eligible for DSA copy.
    pub min_message_bytes: usize,
}

impl DsaConfig {
    /// Creates a DSA configuration that accelerates every non-empty encoded message.
    pub fn new(device_path: impl Into<PathBuf>) -> Self {
        Self {
            device_path: device_path.into(),
            min_message_bytes: DEFAULT_DSA_MIN_MESSAGE_BYTES,
        }
    }

    /// Sets the minimum gRPC message payload size eligible for DSA copy.
    pub fn with_min_message_bytes(mut self, min_message_bytes: usize) -> Self {
        self.min_message_bytes = min_message_bytes;
        self
    }

    fn validate(&self) -> Result<(), DsaConfigError> {
        if self.device_path.as_os_str().is_empty() {
            return Err(DsaConfigError::EmptyDevicePath);
        }
        Ok(())
    }

    fn accelerates(&self, encoded_len: usize) -> bool {
        encoded_len > 0 && encoded_len >= self.min_message_bytes
    }
}

/// DSA shared work-queue configuration or initialization failure.
#[derive(Debug)]
pub enum DsaConfigError {
    /// The work-queue device path was empty.
    EmptyDevicePath,
    /// The process-wide work queue was already initialized.
    AlreadyInitialized,
    /// The process-wide work queue mutex was poisoned.
    ProcessSlotPoisoned,
    /// The configured work-queue device could not be opened.
    Open {
        /// Device path that failed to open.
        device_path: PathBuf,
        /// Underlying OS error from opening the work queue.
        source: std::io::Error,
    },
}

impl fmt::Display for DsaConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DsaConfigError::EmptyDevicePath => {
                f.write_str("async dsa bytes encode requires a non-empty device path")
            }
            DsaConfigError::AlreadyInitialized => {
                f.write_str("process async DSA work queue is already initialized")
            }
            DsaConfigError::ProcessSlotPoisoned => {
                f.write_str("process async DSA work queue slot is poisoned")
            }
            DsaConfigError::Open {
                device_path,
                source,
            } => write!(
                f,
                "async dsa bytes encode failed to open {}: {source}",
                device_path.display()
            ),
        }
    }
}

impl std::error::Error for DsaConfigError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            DsaConfigError::Open { source, .. } => Some(source),
            DsaConfigError::EmptyDevicePath
            | DsaConfigError::AlreadyInitialized
            | DsaConfigError::ProcessSlotPoisoned => None,
        }
    }
}

/// Shared DSA work queue used by asynchronous DSA bytes encoders.
pub struct DsaWorkQueue {
    config: DsaConfig,
    portal: WqPortal,
    dedicated: bool,
}

impl fmt::Debug for DsaWorkQueue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DsaWorkQueue")
            .field("config", &self.config)
            .field("dedicated", &self.dedicated)
            .finish_non_exhaustive()
    }
}

impl DsaWorkQueue {
    /// Opens a DSA work queue and returns a shareable handle for encoders.
    pub fn open(config: DsaConfig) -> Result<SharedDsaWorkQueue, DsaConfigError> {
        config.validate()?;
        let dedicated = detect_wq_mode(&config.device_path);
        let portal =
            WqPortal::open(&config.device_path).map_err(|source| DsaConfigError::Open {
                device_path: config.device_path.clone(),
                source,
            })?;
        Ok(Arc::new(Self {
            config,
            portal,
            dedicated,
        }))
    }

    /// Returns the work-queue configuration.
    pub fn config(&self) -> &DsaConfig {
        &self.config
    }

    fn accelerates(&self, encoded_len: usize) -> bool {
        self.config.accelerates(encoded_len)
    }

    fn submit(&self, desc: &DsaHwDesc) {
        // SAFETY: `PendingDsaCopy` keeps the descriptor, completion record,
        // source bytes, and destination buffer alive until the completion record
        // reaches a non-zero terminal status. `self.dedicated` is detected from
        // the configured work-queue device.
        unsafe { self.portal.submit_dsa(desc, self.dedicated) };
    }
}

/// Opens and installs the process-wide DSA work queue used by default codecs and encoders.
pub fn set_process_dsa_config(config: DsaConfig) -> Result<(), DsaConfigError> {
    set_process_dsa_work_queue(DsaWorkQueue::open(config)?)
}

/// Installs an already-opened process-wide DSA work queue.
pub fn set_process_dsa_work_queue(work_queue: SharedDsaWorkQueue) -> Result<(), DsaConfigError> {
    let mut slot = PROCESS_DSA_WORK_QUEUE
        .lock()
        .map_err(|_| DsaConfigError::ProcessSlotPoisoned)?;
    if slot.is_some() {
        return Err(DsaConfigError::AlreadyInitialized);
    }
    *slot = Some(work_queue);
    Ok(())
}

/// Returns the current process-wide DSA work queue.
pub fn process_dsa_work_queue() -> Option<SharedDsaWorkQueue> {
    PROCESS_DSA_WORK_QUEUE
        .lock()
        .ok()
        .and_then(|slot| slot.clone())
}

/// Returns the current process-wide DSA configuration.
pub fn configured_process_dsa_config() -> Option<DsaConfig> {
    process_dsa_work_queue().map(|work_queue| work_queue.config().clone())
}

/// A tonic codec that sends and receives raw [`Bytes`] payloads.
///
/// The codec still uses tonic's normal gRPC message framing and HTTP/2 body path.
/// It treats each `Bytes` value as the complete message payload and, when
/// configured, uses asynchronous DSA memmove descriptors for the encode copy.
#[derive(Debug, Clone)]
pub struct DsaAsyncBytesCodec {
    work_queue: Option<SharedDsaWorkQueue>,
}

impl DsaAsyncBytesCodec {
    /// Creates a raw bytes codec using the process DSA work queue when configured.
    pub fn new() -> Self {
        Self::with_optional_work_queue(process_dsa_work_queue())
    }

    /// Creates a raw bytes codec with an explicit shared DSA work queue.
    pub fn with_work_queue(work_queue: SharedDsaWorkQueue) -> Self {
        Self::with_optional_work_queue(Some(work_queue))
    }

    /// Creates a raw bytes codec that keeps the async DSA type but encodes on the CPU path.
    pub fn without_work_queue() -> Self {
        Self::with_optional_work_queue(None)
    }

    fn with_optional_work_queue(work_queue: Option<SharedDsaWorkQueue>) -> Self {
        Self { work_queue }
    }

    /// Builds a raw asynchronous DSA bytes encoder with explicit tonic buffer settings.
    pub fn raw_encoder(buffer_settings: BufferSettings) -> DsaAsyncBytesEncoder {
        DsaAsyncBytesEncoder::new(buffer_settings)
    }

    /// Builds a raw asynchronous DSA bytes encoder with an explicit shared work queue.
    pub fn raw_encoder_with_work_queue(
        buffer_settings: BufferSettings,
        work_queue: SharedDsaWorkQueue,
    ) -> DsaAsyncBytesEncoder {
        DsaAsyncBytesEncoder::with_work_queue(buffer_settings, work_queue)
    }

    /// Builds a raw bytes decoder with explicit tonic buffer settings.
    pub fn raw_decoder(buffer_settings: BufferSettings) -> DsaAsyncBytesDecoder {
        DsaAsyncBytesDecoder::new(buffer_settings)
    }
}

impl Default for DsaAsyncBytesCodec {
    fn default() -> Self {
        Self::new()
    }
}

impl Codec for DsaAsyncBytesCodec {
    type Encode = Bytes;
    type Decode = Bytes;

    type Encoder = DsaAsyncBytesEncoder;
    type Decoder = DsaAsyncBytesDecoder;

    fn encoder(&mut self) -> Self::Encoder {
        DsaAsyncBytesEncoder::with_optional_work_queue(
            BufferSettings::default(),
            self.work_queue.clone(),
        )
    }

    fn decoder(&mut self) -> Self::Decoder {
        DsaAsyncBytesDecoder::new(BufferSettings::default())
    }
}

/// A raw bytes encoder that can asynchronously copy payload bytes with DSA.
pub struct DsaAsyncBytesEncoder {
    buffer_settings: BufferSettings,
    work_queue: Option<SharedDsaWorkQueue>,
    pending: Option<PendingCopy>,
}

impl fmt::Debug for DsaAsyncBytesEncoder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DsaAsyncBytesEncoder")
            .field("buffer_settings", &self.buffer_settings)
            .field("work_queue", &self.work_queue)
            .field("pending", &self.pending.is_some())
            .finish()
    }
}

impl Default for DsaAsyncBytesEncoder {
    fn default() -> Self {
        Self::new(BufferSettings::default())
    }
}

impl DsaAsyncBytesEncoder {
    /// Gets a new raw bytes encoder using the process work queue when configured.
    pub fn new(buffer_settings: BufferSettings) -> Self {
        Self::with_optional_work_queue(buffer_settings, process_dsa_work_queue())
    }

    /// Gets a new raw bytes encoder with an explicit shared work queue.
    pub fn with_work_queue(
        buffer_settings: BufferSettings,
        work_queue: SharedDsaWorkQueue,
    ) -> Self {
        Self::with_optional_work_queue(buffer_settings, Some(work_queue))
    }

    /// Gets an encoder that keeps the async DSA type but encodes on the CPU path.
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
            pending: None,
        }
    }

    #[inline]
    fn should_accelerate(&self, payload_len: usize) -> bool {
        self.work_queue
            .as_ref()
            .is_some_and(|work_queue| work_queue.accelerates(payload_len))
    }

    #[inline]
    fn poll_pending(
        &mut self,
        cx: &mut Context<'_>,
        item: &mut Option<Bytes>,
        dst: EncodeBuf<'_>,
    ) -> Poll<Result<(), Status>> {
        let result = match self
            .pending
            .as_mut()
            .expect("pending encode state available")
            .poll(cx, dst)
        {
            Poll::Ready(result) => result,
            Poll::Pending => return Poll::Pending,
        };

        let _pending = self.pending.take().expect("pending encode state available");
        let _encoded_item = item.take().expect("encoder item available");

        Poll::Ready(result)
    }
}

impl Encoder for DsaAsyncBytesEncoder {
    type Item = Bytes;
    type Error = Status;

    #[inline]
    fn poll_encode(
        &mut self,
        cx: &mut Context<'_>,
        item: &mut Option<Self::Item>,
        mut dst: EncodeBuf<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        if self.pending.is_some() {
            return self.poll_pending(cx, item, dst);
        }

        let payload = item.as_ref().expect("encoder item available");
        let payload_len = payload.len();
        if !self.should_accelerate(payload_len) {
            let item = item.take().expect("encoder item available");
            dst.put(item);
            return Poll::Ready(Ok(()));
        }

        if payload_len > u32::MAX as usize {
            let _encoded_item = item.take().expect("encoder item available");
            return Poll::Ready(Err(Status::internal(format!(
                "async dsa bytes encode cannot copy {payload_len} bytes; maximum DSA transfer is {} bytes",
                u32::MAX
            ))));
        }

        let work_queue = self
            .work_queue
            .as_ref()
            .expect("DSA work queue checked before encoding")
            .clone();
        self.pending = Some(PendingCopy::dsa(work_queue, payload.clone()));
        self.poll_pending(cx, item, dst)
    }

    #[inline]
    fn buffer_settings(&self) -> BufferSettings {
        self.buffer_settings
    }
}

/// A raw bytes decoder for [`DsaAsyncBytesCodec`].
#[derive(Debug, Clone)]
pub struct DsaAsyncBytesDecoder {
    buffer_settings: BufferSettings,
}

impl DsaAsyncBytesDecoder {
    /// Gets a new raw bytes decoder with explicit tonic buffer settings.
    pub fn new(buffer_settings: BufferSettings) -> Self {
        Self { buffer_settings }
    }
}

impl Default for DsaAsyncBytesDecoder {
    fn default() -> Self {
        Self::new(BufferSettings::default())
    }
}

impl Decoder for DsaAsyncBytesDecoder {
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

enum PendingCopy {
    Dsa(Pin<Box<PendingDsaCopy>>),
    #[cfg(test)]
    YieldingCpu(PendingYieldingCpuCopy),
}

impl PendingCopy {
    fn dsa(work_queue: SharedDsaWorkQueue, source: Bytes) -> Self {
        Self::Dsa(PendingDsaCopy::new(work_queue, source))
    }

    #[cfg(test)]
    fn yielding_cpu(source: Bytes) -> Self {
        Self::YieldingCpu(PendingYieldingCpuCopy::new(source))
    }

    fn poll(&mut self, cx: &mut Context<'_>, dst: EncodeBuf<'_>) -> Poll<Result<(), Status>> {
        match self {
            PendingCopy::Dsa(copy) => copy.as_mut().poll(cx, dst),
            #[cfg(test)]
            PendingCopy::YieldingCpu(copy) => copy.poll(cx, dst),
        }
    }
}

struct PendingDsaCopy {
    work_queue: SharedDsaWorkQueue,
    source: Bytes,
    dst: *mut u8,
    dst_len: usize,
    desc: DsaHwDesc,
    completion: DsaCompletionRecord,
    submitted: bool,
    completed: bool,
    _pin: PhantomPinned,
}

// SAFETY: `dst` points into the `BytesMut` allocation owned by Tonic's
// `EncodeBody`. The direct-DMA experiment assumes that allocation remains stable
// while the pending encode is moved between executor threads. `Drop` drains a
// submitted descriptor before the pending state can be released.
unsafe impl Send for PendingDsaCopy {}

impl PendingDsaCopy {
    fn new(work_queue: SharedDsaWorkQueue, source: Bytes) -> Pin<Box<Self>> {
        Box::pin(Self {
            work_queue,
            source,
            dst: std::ptr::null_mut(),
            dst_len: 0,
            desc: DsaHwDesc::default(),
            completion: DsaCompletionRecord::default(),
            submitted: false,
            completed: false,
            _pin: PhantomPinned,
        })
    }

    fn poll(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        mut dst: EncodeBuf<'_>,
    ) -> Poll<Result<(), Status>> {
        let this = self.as_mut().project_mut();

        if !*this.submitted {
            let len = this.source.len();
            // SAFETY: This experiment assumes Tonic's encode buffer allocation
            // remains stable while `poll_encode` returns `Pending`. The pending
            // state stores the pointer only until completion and advances the
            // same reservation exactly once on success.
            let dst_ptr = unsafe { dst.reserve_uninit_slice_for_pending(len) };
            touch_pages_for_dsa(this.source.as_ptr(), dst_ptr, len);

            this.completion.clear();
            this.desc
                .fill_memmove(this.source.as_ptr(), dst_ptr, len as u32);
            this.desc.set_completion(this.completion);
            this.work_queue.submit(this.desc);
            *this.dst = dst_ptr;
            *this.dst_len = len;
            *this.submitted = true;
        }

        if this.completion.status() == DsaCompletionStatus::None.as_u8() {
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }

        *this.completed = true;
        match ensure_dsa_success(
            *this.completion,
            this.source.len(),
            &this.work_queue.config.device_path,
        ) {
            Ok(()) => {
                // SAFETY: DSA completion with success means the descriptor has
                // initialized the exact bytes in the reservation made on the
                // first poll. This is the single matching commit for that
                // reservation.
                unsafe {
                    dst.advance_reserved_uninit_slice(*this.dst_len);
                }
                Poll::Ready(Ok(()))
            }
            Err(status) => Poll::Ready(Err(status)),
        }
    }

    fn drain_completion(&mut self) {
        if !self.submitted || self.completed {
            return;
        }

        while self.completion.status() == DsaCompletionStatus::None.as_u8() {
            core::hint::spin_loop();
        }
        self.completed = true;
    }

    fn project_mut(self: Pin<&mut Self>) -> PendingDsaCopyProjection<'_> {
        // SAFETY: `PendingDsaCopy` is pinned in a `Box`, and this method never
        // moves any field out of the pinned allocation. Mutable references are
        // used only to update descriptor/completion bytes and scalar state in
        // place while their addresses remain stable.
        let this = unsafe { self.get_unchecked_mut() };
        PendingDsaCopyProjection {
            work_queue: &this.work_queue,
            source: &this.source,
            dst: &mut this.dst,
            dst_len: &mut this.dst_len,
            desc: &mut this.desc,
            completion: &mut this.completion,
            submitted: &mut this.submitted,
            completed: &mut this.completed,
        }
    }
}

impl Drop for PendingDsaCopy {
    fn drop(&mut self) {
        self.drain_completion();
    }
}

struct PendingDsaCopyProjection<'a> {
    work_queue: &'a DsaWorkQueue,
    source: &'a Bytes,
    dst: &'a mut *mut u8,
    dst_len: &'a mut usize,
    desc: &'a mut DsaHwDesc,
    completion: &'a mut DsaCompletionRecord,
    submitted: &'a mut bool,
    completed: &'a mut bool,
}

#[cfg(test)]
struct PendingYieldingCpuCopy {
    source: Option<Bytes>,
    yielded: bool,
}

#[cfg(test)]
impl PendingYieldingCpuCopy {
    fn new(source: Bytes) -> Self {
        Self {
            source: Some(source),
            yielded: false,
        }
    }

    fn poll(&mut self, cx: &mut Context<'_>, mut dst: EncodeBuf<'_>) -> Poll<Result<(), Status>> {
        if !self.yielded {
            self.yielded = true;
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }

        dst.put(self.source.take().expect("pending source available"));
        Poll::Ready(Ok(()))
    }
}

fn ensure_dsa_success(
    completion: DsaCompletionRecord,
    encoded_len: usize,
    device_path: &Path,
) -> Result<(), Status> {
    let raw_status = completion.status();
    let status = DsaCompletionStatus::mask(raw_status);
    if status == DsaCompletionStatus::Success.as_u8() {
        return Ok(());
    }

    Err(Status::internal(format!(
        "async dsa bytes encode failed on {} for {encoded_len} bytes: status={raw_status:#04x} result={:#04x} bytes_completed={} fault_addr={:#x}",
        device_path.display(),
        completion.result(),
        completion.bytes_completed(),
        completion.fault_addr()
    )))
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

    impl DsaAsyncBytesEncoder {
        fn yielding_cpu_for_tests(buffer_settings: BufferSettings) -> Self {
            Self {
                buffer_settings,
                work_queue: None,
                pending: None,
            }
        }

        fn start_yielding_cpu_for_tests(&mut self, source: Bytes) {
            self.pending = Some(PendingCopy::yielding_cpu(source));
        }
    }

    #[test]
    fn codec_builds_raw_encoder_and_decoder_without_process_work_queue() {
        let mut codec = DsaAsyncBytesCodec::without_work_queue();
        let _encoder = codec.encoder();
        let _decoder = codec.decoder();
    }

    #[test]
    fn encoder_without_work_queue_uses_cpu_path() {
        let payload = Bytes::from_static(b"raw bytes payload");
        let expected_payload = payload.clone();
        let source = tokio_stream::iter(std::iter::once(Ok::<_, Status>(payload)));
        let mut body = pin!(EncodeBody::new_client(
            DsaAsyncBytesEncoder::without_work_queue(BufferSettings::default()),
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

    #[test]
    fn encode_body_polls_pending_encoder_to_completion() {
        let payload = Bytes::from_static(b"async payload");
        let expected_payload = payload.clone();
        let source = tokio_stream::iter(std::iter::once(Ok::<_, Status>(payload.clone())));
        let mut encoder = DsaAsyncBytesEncoder::yielding_cpu_for_tests(BufferSettings::default());
        encoder.start_yielding_cpu_for_tests(payload);
        let mut body = pin!(EncodeBody::new_client(encoder, source, None, None));
        let waker = std::task::Waker::noop();
        let mut cx = Context::from_waker(waker);

        match body.as_mut().poll_frame(&mut cx) {
            Poll::Pending => {}
            Poll::Ready(other) => panic!("expected pending first poll, got {other:?}"),
        }

        let frame = match body.as_mut().poll_frame(&mut cx) {
            Poll::Ready(Some(Ok(frame))) => frame,
            Poll::Ready(Some(Err(status))) => panic!("encode failed: {status}"),
            Poll::Ready(None) => panic!("body ended before data"),
            Poll::Pending => panic!("pending encode did not complete on second poll"),
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
