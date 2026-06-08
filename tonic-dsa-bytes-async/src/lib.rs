//! Experimental asynchronous DSA bytes codec for tonic.
//!
//! This crate exercises tonic's `Encoder::poll_encode` path with an explicit
//! in-flight Intel DSA copy. The encoder keeps ordinary CPU encoding as the
//! default when no work queue is configured. When DSA is enabled, it submits a
//! memmove descriptor, returns `Poll::Pending`, and finishes the gRPC frame after
//! the descriptor completion record reaches a terminal status.
//!
//! The implementation transfers owned encode storage into the encoder before
//! polling. Pending DSA work keeps that storage and its direct-DMA pointer inside
//! encoder-owned state until completion.

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
    path::PathBuf,
    pin::Pin,
    sync::{Arc, LazyLock, Mutex},
    task::{Context, Poll},
};
use tonic::Status;
use tonic::codec::{BufferSettings, Codec, DecodeBuf, Decoder, EncodeBuffer, Encoder};

const DEFAULT_DSA_MIN_MESSAGE_BYTES: usize = 1;
const PAGE_SIZE: usize = 4096;
const DSA_COMP_STATUS_WRITE: u8 = 0x80;
const MAX_NO_PROGRESS_PAGE_FAULT_RETRIES: usize = 4;

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
    state: EncodeState,
    #[cfg(test)]
    yielding_cpu_for_tests: bool,
}

#[derive(Debug)]
enum EncodeState {
    Idle,
    Ready(EncodeBuffer),
    Pending(PendingCopy),
}

impl fmt::Debug for DsaAsyncBytesEncoder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DsaAsyncBytesEncoder")
            .field("buffer_settings", &self.buffer_settings)
            .field("work_queue", &self.work_queue)
            .field("state", &self.state)
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
            state: EncodeState::Idle,
            #[cfg(test)]
            yielding_cpu_for_tests: false,
        }
    }

    #[inline]
    fn should_accelerate(&self, payload_len: usize) -> bool {
        self.work_queue
            .as_ref()
            .is_some_and(|work_queue| work_queue.accelerates(payload_len))
    }
}

impl Encoder for DsaAsyncBytesEncoder {
    type Item = Bytes;
    type Error = Status;

    #[inline]
    fn start_encode(
        self: Pin<&mut Self>,
        item: Self::Item,
        mut dst: EncodeBuffer,
    ) -> Result<(), Self::Error> {
        let this = self.get_mut();
        if !matches!(this.state, EncodeState::Idle) {
            return Err(Status::internal(
                "async dsa bytes encoder already has an in-flight encode",
            ));
        }

        let payload_len = item.len();

        #[cfg(test)]
        if this.yielding_cpu_for_tests {
            this.state = EncodeState::Pending(PendingCopy::yielding_cpu(item, dst));
            return Ok(());
        }

        if !this.should_accelerate(payload_len) {
            dst.as_encode_buf().put(item);
            this.state = EncodeState::Ready(dst);
            return Ok(());
        }

        if payload_len > u32::MAX as usize {
            return Err(Status::internal(format!(
                "async dsa bytes encode cannot copy {payload_len} bytes; maximum DSA transfer is {} bytes",
                u32::MAX
            )));
        }

        let work_queue = this
            .work_queue
            .as_ref()
            .expect("DSA work queue checked before encoding")
            .clone();
        this.state = EncodeState::Pending(PendingCopy::dsa(work_queue, item, dst));
        Ok(())
    }

    #[inline]
    fn poll_encode(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<EncodeBuffer, Self::Error>> {
        let this = self.get_mut();
        match &mut this.state {
            EncodeState::Idle => panic!("poll_encode without pending item"),
            EncodeState::Ready(_) => {
                let EncodeState::Ready(buf) = std::mem::replace(&mut this.state, EncodeState::Idle)
                else {
                    unreachable!("ready state checked above");
                };
                Poll::Ready(Ok(buf))
            }
            EncodeState::Pending(pending) => match pending.poll(cx) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(result) => {
                    this.state = EncodeState::Idle;
                    Poll::Ready(result)
                }
            },
        }
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

impl fmt::Debug for PendingCopy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PendingCopy::Dsa(_) => f.write_str("PendingCopy::Dsa"),
            #[cfg(test)]
            PendingCopy::YieldingCpu(_) => f.write_str("PendingCopy::YieldingCpu"),
        }
    }
}

impl PendingCopy {
    fn dsa(work_queue: SharedDsaWorkQueue, source: Bytes, buf: EncodeBuffer) -> Self {
        Self::Dsa(PendingDsaCopy::new(work_queue, source, buf))
    }

    #[cfg(test)]
    fn yielding_cpu(source: Bytes, buf: EncodeBuffer) -> Self {
        Self::YieldingCpu(PendingYieldingCpuCopy::new(source, buf))
    }

    fn poll(&mut self, cx: &mut Context<'_>) -> Poll<Result<EncodeBuffer, Status>> {
        match self {
            PendingCopy::Dsa(copy) => copy.as_mut().poll(cx),
            #[cfg(test)]
            PendingCopy::YieldingCpu(copy) => copy.poll(cx),
        }
    }
}

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
                "async dsa bytes encode cannot copy remaining {} bytes; maximum DSA transfer is {} bytes",
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
        device_path: &PathBuf,
    ) -> Result<DsaRetryAction, Status> {
        let raw_status = completion.status();
        let status = DsaCompletionStatus::mask(raw_status);
        if status == DsaCompletionStatus::Success.as_u8() {
            return Ok(DsaRetryAction::Complete);
        }

        if status != DsaCompletionStatus::PageFaultNoBof.as_u8() {
            return Err(dsa_completion_error(
                "async dsa bytes encode failed",
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
        device_path: &PathBuf,
    ) -> Result<(), Status> {
        if self.retry_count >= self.max_retries {
            return Err(Status::internal(format!(
                "async dsa bytes encode page-fault retry limit exceeded on {} for {} bytes: retries={} remaining={} fault_addr={fault_addr:#x}",
                device_path.display(),
                self.original_len,
                self.retry_count,
                self.remaining
            )));
        }

        let completed = bytes_completed as usize;
        if completed > self.remaining {
            return Err(Status::internal(format!(
                "async dsa bytes encode page fault on {} reported bytes_completed={} beyond remaining {} for {} byte transfer",
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
                    "async dsa bytes encode made no progress after {} page-fault retries on {} for {} bytes: remaining={} fault_addr={fault_addr:#x}",
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
                "async dsa bytes encode page fault on {} left no remaining bytes to retry for {} byte transfer",
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

struct PendingDsaCopy {
    work_queue: SharedDsaWorkQueue,
    source: Bytes,
    buf: Option<EncodeBuffer>,
    dst: *mut u8,
    dst_len: usize,
    desc: DsaHwDesc,
    completion: DsaCompletionRecord,
    retry: Option<DsaMemmoveRetry>,
    submitted: bool,
    completed: bool,
    _pin: PhantomPinned,
}

// SAFETY: `dst` points into the `EncodeBuffer` owned by this pending state.
// Both the pointer and the buffer move together inside the boxed pending copy.
// `Drop` drains a submitted descriptor before the pending state can be released.
unsafe impl Send for PendingDsaCopy {}

impl PendingDsaCopy {
    fn new(work_queue: SharedDsaWorkQueue, source: Bytes, buf: EncodeBuffer) -> Pin<Box<Self>> {
        Box::pin(Self {
            work_queue,
            source,
            buf: Some(buf),
            dst: std::ptr::null_mut(),
            dst_len: 0,
            desc: DsaHwDesc::default(),
            completion: DsaCompletionRecord::default(),
            retry: None,
            submitted: false,
            completed: false,
            _pin: PhantomPinned,
        })
    }

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<EncodeBuffer, Status>> {
        let this = self.as_mut().project_mut();

        if !*this.submitted {
            let len = this.source.len();
            let buf = this.buf.as_mut().expect("pending encode buffer available");
            let mut dst = buf.as_encode_buf();
            // SAFETY: The pending state owns the encode buffer until completion.
            // The direct-DMA pointer is retained only while this pending state is
            // alive, and the matching commit happens exactly once on success.
            let dst_ptr = unsafe { dst.reserve_uninit_slice_for_pending(len) };
            touch_pages_for_dsa(this.source.as_ptr(), dst_ptr, len);

            let retry = DsaMemmoveRetry::new(this.source.as_ptr(), dst_ptr, len);
            submit_retry(this.work_queue, this.desc, this.completion, &retry)?;
            *this.retry = Some(retry);
            *this.dst = dst_ptr;
            *this.dst_len = len;
            *this.submitted = true;
        }

        if this.completion.status() == DsaCompletionStatus::None.as_u8() {
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }

        let retry = this.retry.as_mut().expect("pending retry state available");
        match retry.handle_completion(*this.completion, &this.work_queue.config.device_path) {
            Ok(DsaRetryAction::Complete) => {
                *this.completed = true;
                let mut buf = this.buf.take().expect("pending encode buffer available");
                // SAFETY: DSA completion with success means the descriptor has
                // initialized the exact bytes in the reservation made on the
                // first poll. This is the single matching commit for that
                // reservation.
                unsafe {
                    buf.as_encode_buf()
                        .advance_reserved_uninit_slice(*this.dst_len);
                }
                Poll::Ready(Ok(buf))
            }
            Ok(DsaRetryAction::Retry) => {
                submit_retry(this.work_queue, this.desc, this.completion, retry)?;
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            Err(status) => {
                *this.completed = true;
                Poll::Ready(Err(status))
            }
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
        // moves any field out of the pinned allocation except through explicit
        // `Option::take` on the owned encode buffer after hardware completion.
        let this = unsafe { self.get_unchecked_mut() };
        PendingDsaCopyProjection {
            work_queue: &this.work_queue,
            source: &this.source,
            buf: &mut this.buf,
            dst: &mut this.dst,
            dst_len: &mut this.dst_len,
            desc: &mut this.desc,
            retry: &mut this.retry,
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
    buf: &'a mut Option<EncodeBuffer>,
    dst: &'a mut *mut u8,
    dst_len: &'a mut usize,
    desc: &'a mut DsaHwDesc,
    completion: &'a mut DsaCompletionRecord,
    retry: &'a mut Option<DsaMemmoveRetry>,
    submitted: &'a mut bool,
    completed: &'a mut bool,
}

#[cfg(test)]
struct PendingYieldingCpuCopy {
    source: Option<Bytes>,
    buf: Option<EncodeBuffer>,
    yielded: bool,
}

#[cfg(test)]
impl PendingYieldingCpuCopy {
    fn new(source: Bytes, buf: EncodeBuffer) -> Self {
        Self {
            source: Some(source),
            buf: Some(buf),
            yielded: false,
        }
    }

    fn poll(&mut self, cx: &mut Context<'_>) -> Poll<Result<EncodeBuffer, Status>> {
        if !self.yielded {
            self.yielded = true;
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }

        let mut buf = self.buf.take().expect("pending encode buffer available");
        buf.as_encode_buf()
            .put(self.source.take().expect("pending source available"));
        Poll::Ready(Ok(buf))
    }
}

fn submit_retry(
    work_queue: &DsaWorkQueue,
    desc: &mut DsaHwDesc,
    completion: &mut DsaCompletionRecord,
    retry: &DsaMemmoveRetry,
) -> Result<(), Status> {
    completion.clear();
    retry.fill_desc(desc)?;
    desc.set_completion(completion);
    work_queue.submit(desc);
    Ok(())
}

fn dsa_completion_error(
    context: &str,
    completion: DsaCompletionRecord,
    encoded_len: usize,
    remaining: usize,
    device_path: &PathBuf,
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
            "async dsa bytes encode page fault reported a null fault address",
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
    use std::pin::pin;
    use tonic::codec::{EncodeBody, HEADER_SIZE};

    #[test]
    fn page_fault_retry_advances_memmove_descriptor() {
        let src = [0x5a; 512];
        let mut dst = [0u8; 512];
        let device_path = PathBuf::from("/dev/dsa/test");
        let mut retry = DsaMemmoveRetry::new(src.as_ptr(), dst.as_mut_ptr(), src.len());
        let fault_addr = dst.as_mut_ptr() as u64;

        retry
            .handle_page_fault(
                DSA_COMP_STATUS_WRITE | DsaCompletionStatus::PageFaultNoBof.as_u8(),
                128,
                fault_addr,
                &device_path,
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
        let device_path = PathBuf::from("/dev/dsa/test");
        let mut retry = DsaMemmoveRetry::new(src.as_ptr(), dst.as_mut_ptr(), src.len());
        let fault_addr = src.as_ptr() as u64;

        for _ in 0..MAX_NO_PROGRESS_PAGE_FAULT_RETRIES {
            retry
                .handle_page_fault(
                    DsaCompletionStatus::PageFaultNoBof.as_u8(),
                    0,
                    fault_addr,
                    &device_path,
                )
                .expect("bounded no-progress retry allowed");
        }

        let err = retry
            .handle_page_fault(
                DsaCompletionStatus::PageFaultNoBof.as_u8(),
                0,
                fault_addr,
                &device_path,
            )
            .expect_err("repeated no-progress retry rejected");
        assert!(err.to_string().contains("made no progress"));
    }

    impl DsaAsyncBytesEncoder {
        fn yielding_cpu_for_tests(buffer_settings: BufferSettings) -> Self {
            Self {
                buffer_settings,
                work_queue: None,
                state: EncodeState::Idle,
                yielding_cpu_for_tests: true,
            }
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
        let encoder = DsaAsyncBytesEncoder::yielding_cpu_for_tests(BufferSettings::default());
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
