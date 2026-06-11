//! Experimental synchronous DSA bytes codec for tonic.
//!
//! This crate exposes a raw `Buf`/`Bytes` codec for research runs that want to
//! route encode copies through Intel DSA without adding hardware-specific code
//! to tonic itself. The implementation is intentionally synchronous: after
//! submitting DSA memmove descriptors to a shared work queue, the encoder spins
//! until completion records report terminal statuses.

#![warn(
    missing_docs,
    missing_debug_implementations,
    rust_2018_idioms,
    unreachable_pub
)]
#![doc(
    html_logo_url = "https://raw.githubusercontent.com/tokio-rs/website/master/public/img/icons/tonic.svg"
)]
#![doc(issue_tracker_base_url = "https://github.com/hyperium/tonic/issues/")]

mod dsa_buf;

pub use dsa_buf::{DsaBufCodec, DsaSyncBufEncoder, DsaSyncBytesDecoder};

use idxd_rust::{DsaCompletionRecord, DsaEngine, DsaHwDesc};
use std::{
    fmt,
    path::PathBuf,
    sync::{Arc, OnceLock},
    task::{Context, Poll},
};

const DEFAULT_DSA_MIN_MESSAGE_BYTES: usize = 1;

static PROCESS_DSA_WORK_QUEUE: OnceLock<SharedDsaWorkQueue> = OnceLock::new();

/// Shared DSA work queue handle used by DSA sync encoders.
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
                f.write_str("dsa bytes encode requires a non-empty device path")
            }
            DsaConfigError::AlreadyInitialized => {
                f.write_str("process DSA work queue is already initialized")
            }
            DsaConfigError::Open {
                device_path,
                source,
            } => write!(
                f,
                "dsa bytes encode failed to open {}: {source}",
                device_path.display()
            ),
        }
    }
}

impl std::error::Error for DsaConfigError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            DsaConfigError::Open { source, .. } => Some(source),
            DsaConfigError::EmptyDevicePath | DsaConfigError::AlreadyInitialized => None,
        }
    }
}

/// Shared DSA work queue used by synchronous DSA bytes encoders.
pub struct DsaWorkQueue {
    config: DsaConfig,
    engine: DsaEngine,
}

impl fmt::Debug for DsaWorkQueue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DsaWorkQueue")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl DsaWorkQueue {
    /// Opens a DSA work queue and returns a shareable handle for encoders.
    pub fn open(config: DsaConfig) -> Result<SharedDsaWorkQueue, DsaConfigError> {
        config.validate()?;
        let engine =
            DsaEngine::open(&config.device_path).map_err(|source| DsaConfigError::Open {
                device_path: config.device_path.clone(),
                source,
            })?;
        Ok(Arc::new(Self { config, engine }))
    }

    /// Returns the work-queue configuration.
    pub fn config(&self) -> &DsaConfig {
        &self.config
    }

    fn accelerates(&self, encoded_len: usize) -> bool {
        self.config.accelerates(encoded_len)
    }
}

/// Opens and installs the process-wide DSA work queue used by default codecs and encoders.
///
/// The process work queue is initialized at most once because it depends on a
/// runtime-selected device path. Construct a codec with [`DsaBufCodec::with_work_queue`]
/// when tests or experiments need an explicit handle instead of the process default.
pub fn set_process_dsa_config(config: DsaConfig) -> Result<(), DsaConfigError> {
    set_process_dsa_work_queue(DsaWorkQueue::open(config)?)
}

/// Installs an already-opened process-wide DSA work queue.
pub fn set_process_dsa_work_queue(work_queue: SharedDsaWorkQueue) -> Result<(), DsaConfigError> {
    PROCESS_DSA_WORK_QUEUE
        .set(work_queue)
        .map_err(|_| DsaConfigError::AlreadyInitialized)
}

/// Returns the current process-wide DSA work queue.
pub fn process_dsa_work_queue() -> Option<SharedDsaWorkQueue> {
    PROCESS_DSA_WORK_QUEUE.get().cloned()
}

/// Returns the current process-wide DSA configuration.
pub fn configured_process_dsa_config() -> Option<DsaConfig> {
    PROCESS_DSA_WORK_QUEUE
        .get()
        .map(|work_queue| work_queue.config().clone())
}

fn poll_dsa_descriptor_to_completion(engine: &DsaEngine, desc: DsaHwDesc) -> DsaCompletionRecord {
    let waker = std::task::Waker::noop();
    let mut cx = Context::from_waker(waker);
    let mut operation = std::pin::pin!(engine.submit_descriptor(desc));

    loop {
        match operation.as_mut().poll(&mut cx) {
            Poll::Ready(completion) => return completion,
            Poll::Pending => core::hint::spin_loop(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tonic::codec::Codec;

    fn assert_send_sync<T: Send + Sync>() {}

    #[test]
    fn work_queue_handle_is_send_sync() {
        assert_send_sync::<DsaWorkQueue>();
        assert_send_sync::<SharedDsaWorkQueue>();
    }

    #[test]
    fn process_config_rejects_empty_device_path() {
        let err = set_process_dsa_config(DsaConfig::new(PathBuf::new()))
            .expect_err("empty path rejected");

        assert!(matches!(err, DsaConfigError::EmptyDevicePath));
        assert_eq!(configured_process_dsa_config(), None);
    }

    #[test]
    fn missing_device_reports_open_error_without_process_install() {
        let err = set_process_dsa_config(
            DsaConfig::new(PathBuf::from("/tmp/tonic-dsa-bytes-missing-wq"))
                .with_min_message_bytes(1),
        )
        .expect_err("missing work queue open fails");

        match err {
            DsaConfigError::Open { device_path, .. } => {
                assert_eq!(
                    device_path,
                    PathBuf::from("/tmp/tonic-dsa-bytes-missing-wq")
                );
            }
            err => panic!("expected open error, got {err:?}"),
        }
        assert_eq!(configured_process_dsa_config(), None);
    }

    #[test]
    fn codec_builds_bytes_encoder_and_decoder_without_process_work_queue() {
        let mut codec = DsaBufCodec::without_work_queue();
        let _encoder = codec.encoder();
        let _decoder = codec.decoder();
    }
}
