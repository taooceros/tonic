//! Opt-in hardware tests for generated Prost async payload copies through DSA.
#![cfg(feature = "hardware-dsa")]

use idxd_rust::{detect_wq_mode, DsaCompletionRecord, DsaCompletionStatus, DsaHwDesc, WqPortal};
use prost::bytes::Bytes;
use prost::transfer::{AsyncEncodeRefExt, AsyncEncodeTarget, EncodePayload};
use prost::Message as _;
use std::{
    collections::BTreeMap,
    env,
    future::Future,
    marker::PhantomPinned,
    path::PathBuf,
    pin::{pin, Pin},
    task::{Context, Poll},
};

const PAGE_SIZE: usize = 4096;

struct HardwareDsaWorkQueue {
    device_path: PathBuf,
    portal: WqPortal,
    dedicated: bool,
}

impl HardwareDsaWorkQueue {
    fn open_from_env() -> Self {
        let device_path = env::var_os("TONIC_DSA_WQ")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("/dev/dsa/wq0.0"));
        assert!(
            device_path.exists(),
            "DSA work-queue device does not exist: {}",
            device_path.display()
        );

        let dedicated = detect_wq_mode(&device_path);
        let portal = WqPortal::open(&device_path)
            .unwrap_or_else(|err| panic!("failed to open {}: {err}", device_path.display()));

        Self {
            device_path,
            portal,
            dedicated,
        }
    }

    fn submit(&self, desc: &DsaHwDesc) {
        // SAFETY: `DsaPayloadState` keeps the descriptor, completion record,
        // source message bytes, and destination buffer alive until hardware
        // completion reaches a terminal status.
        unsafe { self.portal.submit_dsa(desc, self.dedicated) };
    }
}

#[derive(Clone, PartialEq, prost::Message)]
struct HardwareNestedPayload {
    #[prost(uint32, tag = "1")]
    id: u32,
    #[prost(string, tag = "2")]
    name: String,
    #[prost(bytes = "vec", tag = "3")]
    payload: Vec<u8>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct HardwareProstMessage {
    #[prost(uint32, tag = "1")]
    scalar: u32,
    #[prost(string, tag = "2")]
    name: String,
    #[prost(bytes = "bytes", tag = "3")]
    payload: Bytes,
    #[prost(string, repeated, tag = "4")]
    repeated_names: Vec<String>,
    #[prost(bytes = "vec", repeated, tag = "5")]
    repeated_payloads: Vec<Vec<u8>>,
    #[prost(btree_map = "string, string", tag = "6")]
    string_map: BTreeMap<String, String>,
    #[prost(btree_map = "string, bytes", tag = "7")]
    bytes_map: BTreeMap<String, Vec<u8>>,
    #[prost(message, optional, tag = "8")]
    nested: Option<HardwareNestedPayload>,
    #[prost(message, repeated, tag = "9")]
    repeated_nested: Vec<HardwareNestedPayload>,
    #[prost(btree_map = "string, message", tag = "10")]
    nested_map: BTreeMap<String, HardwareNestedPayload>,
    #[prost(oneof = "HardwarePayloadOneof", tags = "11, 12")]
    payload_oneof: Option<HardwarePayloadOneof>,
    #[prost(oneof = "HardwareNestedOneof", tags = "13, 14")]
    nested_oneof: Option<HardwareNestedOneof>,
}

#[derive(Clone, PartialEq, prost::Oneof)]
enum HardwarePayloadOneof {
    #[prost(bytes = "vec", tag = "11")]
    Payload(Vec<u8>),
    #[prost(string, tag = "12")]
    Text(String),
}

#[derive(Clone, PartialEq, prost::Oneof)]
enum HardwareNestedOneof {
    #[prost(message, tag = "13")]
    Nested(HardwareNestedPayload),
    #[prost(string, tag = "14")]
    Text(String),
}

#[test]
fn dsa_encode_async_matches_sync_encode_for_generated_payloads() {
    let work_queue = HardwareDsaWorkQueue::open_from_env();
    let msg = hardware_message();

    let mut expected = Vec::with_capacity(msg.encoded_len());
    msg.encode(&mut expected)
        .expect("synchronous prost encode succeeds");

    let mut actual = Vec::with_capacity(msg.encoded_len());
    let (pending_polls, dsa_payloads) = {
        let mut target = DsaPayloadTarget::new(&work_queue, &mut actual);
        let (result, pending_polls) = poll_until_ready(msg.encode_async_ref(&mut target));
        result.expect("DSA-backed Prost async encode succeeds");
        assert_eq!(target.submitted_payloads, target.completed_payloads);
        (pending_polls, target.completed_payloads)
    };

    assert!(pending_polls > 0, "DSA encode completed without yielding");
    assert!(dsa_payloads > 0, "no Prost string/bytes payload used DSA");
    assert_eq!(actual, expected);

    let decoded = HardwareProstMessage::decode(actual.as_slice()).expect("DSA output decodes");
    assert_eq!(decoded, msg);

    println!(
        "dsa prost encode_async result: payload_len={} pending_polls={} dsa_payloads={} prefix_hex={}",
        actual.len(),
        pending_polls,
        dsa_payloads,
        hex_prefix(&actual, 32)
    );
}

fn hardware_message() -> HardwareProstMessage {
    let nested_a = HardwareNestedPayload {
        id: 7,
        name: "hardware nested a ".repeat(1024),
        payload: vec![0x11; 64 * 1024],
    };
    let nested_b = HardwareNestedPayload {
        id: 8,
        name: "hardware nested b ".repeat(1024),
        payload: vec![0x22; 64 * 1024],
    };
    let nested_c = HardwareNestedPayload {
        id: 9,
        name: "hardware nested c ".repeat(1024),
        payload: vec![0x33; 64 * 1024],
    };

    HardwareProstMessage {
        scalar: 42,
        name: "hardware dsa prost string ".repeat(4096),
        payload: Bytes::from(vec![0xa5; 256 * 1024]),
        repeated_names: vec![
            "hardware repeated string a ".repeat(1024),
            "hardware repeated string b ".repeat(1024),
        ],
        repeated_payloads: vec![vec![0x44; 96 * 1024], vec![0x55; 96 * 1024]],
        string_map: BTreeMap::from([
            (
                "string-map-key-a".to_owned(),
                "hardware string map value a ".repeat(1024),
            ),
            (
                "string-map-key-b".to_owned(),
                "hardware string map value b ".repeat(1024),
            ),
        ]),
        bytes_map: BTreeMap::from([
            ("bytes-map-key-a".to_owned(), vec![0x66; 80 * 1024]),
            ("bytes-map-key-b".to_owned(), vec![0x77; 80 * 1024]),
        ]),
        nested: Some(nested_a.clone()),
        repeated_nested: vec![nested_a.clone(), nested_b.clone()],
        nested_map: BTreeMap::from([
            ("nested-map-key-a".to_owned(), nested_b.clone()),
            ("nested-map-key-b".to_owned(), nested_c.clone()),
        ]),
        payload_oneof: Some(HardwarePayloadOneof::Payload(vec![0x88; 128 * 1024])),
        nested_oneof: Some(HardwareNestedOneof::Nested(nested_c)),
    }
}

#[derive(Debug)]
enum HardwareDsaError {
    Encode(prost::EncodeError),
    TransferTooLarge {
        len: usize,
    },
    Completion {
        device_path: PathBuf,
        status: u8,
        result: u8,
        bytes_completed: u32,
        fault_addr: u64,
    },
}

impl core::fmt::Display for HardwareDsaError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Encode(error) => write!(f, "{error}"),
            Self::TransferTooLarge { len } => {
                write!(f, "DSA payload transfer is larger than u32::MAX: {len}")
            }
            Self::Completion {
                device_path,
                status,
                result,
                bytes_completed,
                fault_addr,
            } => write!(
                f,
                "DSA payload copy failed on {}: status={status:#04x} result={result:#04x} bytes_completed={bytes_completed} fault_addr={fault_addr:#x}",
                device_path.display()
            ),
        }
    }
}

impl std::error::Error for HardwareDsaError {}

struct DsaPayloadTarget<'a> {
    work_queue: &'a HardwareDsaWorkQueue,
    buf: &'a mut Vec<u8>,
    submitted_payloads: usize,
    completed_payloads: usize,
}

impl<'a> DsaPayloadTarget<'a> {
    fn new(work_queue: &'a HardwareDsaWorkQueue, buf: &'a mut Vec<u8>) -> Self {
        Self {
            work_queue,
            buf,
            submitted_payloads: 0,
            completed_payloads: 0,
        }
    }

    fn start_payload_copy(
        &mut self,
        payload: &[u8],
        state: &mut DsaPayloadState,
    ) -> Result<(), HardwareDsaError> {
        let len = payload.len();
        let xfer_size =
            u32::try_from(len).map_err(|_| HardwareDsaError::TransferTooLarge { len })?;
        let dst_offset = self.buf.len();
        self.buf.reserve(len);
        let dst = unsafe { self.buf.as_mut_ptr().add(dst_offset) };

        touch_pages_for_dsa(payload.as_ptr(), dst, len);
        *state = DsaPayloadState::Pending(PendingDsaPayloadCopy {
            desc: DsaHwDesc::default(),
            completion: DsaCompletionRecord::default(),
            dst_offset,
            len,
            submitted: false,
            _pin: PhantomPinned,
        });

        let DsaPayloadState::Pending(pending) = state else {
            unreachable!("pending DSA payload state was just installed")
        };
        pending.completion.clear();
        pending.desc.fill_memmove(payload.as_ptr(), dst, xfer_size);
        pending.desc.set_completion(&mut pending.completion);
        self.work_queue.submit(&pending.desc);
        pending.submitted = true;
        self.submitted_payloads += 1;
        Ok(())
    }
}

impl AsyncEncodeTarget for DsaPayloadTarget<'_> {
    type Error = HardwareDsaError;

    type BufMut<'a>
        = &'a mut Vec<u8>
    where
        Self: 'a;

    type PayloadState = DsaPayloadState;

    fn encode_error(error: prost::EncodeError) -> Self::Error {
        HardwareDsaError::Encode(error)
    }

    fn buf_mut(&mut self) -> Self::BufMut<'_> {
        self.buf
    }

    fn poll_write_payload(
        &mut self,
        payload: EncodePayload<'_>,
        state: Pin<&mut Self::PayloadState>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        let payload = payload.as_bytes();
        // SAFETY: `DsaPayloadState` only becomes self-referential after this
        // pinned poll path installs and submits a pending descriptor.
        let state = unsafe { state.get_unchecked_mut() };
        match state {
            DsaPayloadState::Idle => {
                if payload.is_empty() {
                    return Poll::Ready(Ok(()));
                }
                self.start_payload_copy(payload, state)?;
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            DsaPayloadState::Pending(_) => {
                let completion = {
                    let DsaPayloadState::Pending(pending) = state else {
                        unreachable!("checked pending DSA payload state")
                    };
                    pending.poll_completion(&self.work_queue.device_path)
                };

                match completion {
                    Poll::Pending => {
                        cx.waker().wake_by_ref();
                        Poll::Pending
                    }
                    Poll::Ready(Ok((dst_offset, len))) => {
                        debug_assert_eq!(self.buf.len(), dst_offset);
                        // SAFETY: DSA reached success for exactly the reserved
                        // destination range, so those bytes are initialized.
                        unsafe { self.buf.set_len(dst_offset + len) };
                        self.completed_payloads += 1;
                        *state = DsaPayloadState::Idle;
                        Poll::Ready(Ok(()))
                    }
                    Poll::Ready(Err(error)) => {
                        *state = DsaPayloadState::Idle;
                        Poll::Ready(Err(error))
                    }
                }
            }
        }
    }
}

enum DsaPayloadState {
    Idle,
    Pending(PendingDsaPayloadCopy),
}

impl Default for DsaPayloadState {
    fn default() -> Self {
        Self::Idle
    }
}

struct PendingDsaPayloadCopy {
    desc: DsaHwDesc,
    completion: DsaCompletionRecord,
    dst_offset: usize,
    len: usize,
    submitted: bool,
    _pin: PhantomPinned,
}

impl PendingDsaPayloadCopy {
    fn poll_completion(
        &mut self,
        device_path: &PathBuf,
    ) -> Poll<Result<(usize, usize), HardwareDsaError>> {
        let raw_status = self.completion.status();
        let status = DsaCompletionStatus::mask(raw_status);
        if status == DsaCompletionStatus::None.as_u8() {
            return Poll::Pending;
        }
        if status == DsaCompletionStatus::Success.as_u8() {
            return Poll::Ready(Ok((self.dst_offset, self.len)));
        }

        Poll::Ready(Err(HardwareDsaError::Completion {
            device_path: device_path.clone(),
            status: raw_status,
            result: self.completion.result(),
            bytes_completed: self.completion.bytes_completed(),
            fault_addr: self.completion.fault_addr(),
        }))
    }
}

impl Drop for PendingDsaPayloadCopy {
    fn drop(&mut self) {
        if !self.submitted {
            return;
        }

        while DsaCompletionStatus::mask(self.completion.status())
            == DsaCompletionStatus::None.as_u8()
        {
            core::hint::spin_loop();
        }
    }
}

fn touch_pages_for_dsa(src: *const u8, dst: *mut u8, len: usize) {
    debug_assert!(len != 0);

    let mut offset = 0;
    while offset < len {
        // SAFETY: `offset < len`, so both addresses are inside the source and
        // destination ranges. The destination byte is initialized before DSA
        // writes the final payload bytes.
        unsafe {
            std::ptr::read_volatile(src.add(offset));
            std::ptr::write_volatile(dst.add(offset), 0);
        }
        offset = offset.saturating_add(PAGE_SIZE);
    }

    let last = len - 1;
    // SAFETY: `last < len`; this covers the final byte when the range does not
    // end exactly on a page boundary.
    unsafe {
        std::ptr::read_volatile(src.add(last));
        std::ptr::write_volatile(dst.add(last), 0);
    }
}

fn poll_until_ready<F>(future: F) -> (F::Output, usize)
where
    F: Future,
{
    let mut future = pin!(future);
    let waker = std::task::Waker::noop();
    let mut cx = Context::from_waker(waker);
    let mut pending_polls = 0;

    loop {
        match future.as_mut().poll(&mut cx) {
            Poll::Ready(output) => return (output, pending_polls),
            Poll::Pending => pending_polls += 1,
        }
    }
}

fn hex_prefix(bytes: &[u8], len: usize) -> String {
    let mut out = String::new();
    for byte in bytes.iter().take(len) {
        use core::fmt::Write as _;
        write!(&mut out, "{byte:02x}").expect("write to String");
    }
    out
}
