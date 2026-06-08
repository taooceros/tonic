//! Opt-in hardware smoke tests for the asynchronous DSA bytes encoder.

use bytes::Bytes;
use http_body::Body;
use std::{
    env,
    path::PathBuf,
    pin::pin,
    task::{Context, Poll},
};
use tonic::{
    Status,
    codec::{BufferSettings, EncodeBody, HEADER_SIZE},
};
use tonic_dsa_bytes_async::{DsaAsyncBytesEncoder, DsaConfig, DsaWorkQueue};

#[test]
#[ignore = "requires an enabled /dev/dsa work queue and CAP_SYS_RAWIO"]
fn dsa_work_queue_encodes_one_frame() {
    let device_path = env::var_os("TONIC_DSA_WQ")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/dev/dsa/wq0.0"));
    assert!(
        device_path.exists(),
        "DSA work-queue device does not exist: {}",
        device_path.display()
    );

    let work_queue =
        DsaWorkQueue::open(DsaConfig::new(device_path.clone()).with_min_message_bytes(1))
            .unwrap_or_else(|err| panic!("failed to open {}: {err}", device_path.display()));

    let payload = Bytes::from_static(b"async dsa hardware payload");
    let expected_payload = payload.clone();
    let source = tokio_stream::iter(std::iter::once(Ok::<_, Status>(payload)));
    let mut body = pin!(EncodeBody::new_client(
        DsaAsyncBytesEncoder::with_work_queue(BufferSettings::default(), work_queue),
        source,
        None,
        None,
    ));
    let waker = std::task::Waker::noop();
    let mut cx = Context::from_waker(waker);

    let frame = poll_next_frame(&mut body, &mut cx);
    let data = frame.into_data().expect("got data frame");

    assert_eq!(data[0], 0);
    assert_eq!(
        u32::from_be_bytes(data[1..HEADER_SIZE].try_into().unwrap()) as usize,
        expected_payload.len()
    );
    assert_eq!(&data[HEADER_SIZE..], &expected_payload[..]);
}

fn poll_next_frame<B>(
    body: &mut std::pin::Pin<&mut B>,
    cx: &mut Context<'_>,
) -> http_body::Frame<Bytes>
where
    B: Body<Data = Bytes, Error = Status>,
{
    for _ in 0..1_000_000 {
        match body.as_mut().poll_frame(cx) {
            Poll::Ready(Some(Ok(frame))) => return frame,
            Poll::Ready(Some(Err(status))) => panic!("encode failed: {status}"),
            Poll::Ready(None) => panic!("body ended before data"),
            Poll::Pending => core::hint::spin_loop(),
        }
    }

    panic!("timed out waiting for DSA encode completion")
}
