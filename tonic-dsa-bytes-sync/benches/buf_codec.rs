#![allow(missing_docs)]

use bencher::{Bencher, benchmark_group, benchmark_main, black_box};
use bytes::{Buf, BufMut, Bytes};
use http_body::Body;
use std::{
    pin::pin,
    task::{Context, Poll},
};
use tonic::{Status, codec::BufferSettings, codec::EncodeBody, codec::EncodeBuf, codec::Encoder};
use tonic_dsa_bytes_sync::{DsaConfig, DsaSyncBufEncoder, DsaWorkQueue};

const DSA_WQ_ENV: &str = "TONIC_DSA_WQ";

type BoxBuf = Box<dyn Buf + Send + Sync>;

#[derive(Debug, Clone, Copy)]
struct SoftwareBufEncoder;

impl Encoder for SoftwareBufEncoder {
    type Item = BoxBuf;
    type Error = Status;

    const ENCODE_READY: bool = true;

    #[inline]
    fn encode_ready(
        &mut self,
        mut item: Self::Item,
        mut dst: EncodeBuf<'_>,
    ) -> Result<(), Self::Error> {
        dst.put(&mut *item);
        Ok(())
    }

    #[inline]
    fn poll_encode(
        &mut self,
        _cx: &mut Context<'_>,
        item: &mut Option<Self::Item>,
        mut dst: EncodeBuf<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        let mut item = item.take().expect("encoder item available");
        dst.put(&mut *item);
        Poll::Ready(Ok(()))
    }
}

fn make_payload(len: usize) -> Bytes {
    Bytes::from(vec![0x5a; len])
}

fn encode_one<E>(encoder: E, payload: Bytes) -> Bytes
where
    E: Encoder<Item = BoxBuf, Error = Status> + Send + 'static,
{
    let source = tokio_stream::iter(std::iter::once(
        Ok::<_, Status>(Box::new(payload) as BoxBuf),
    ));
    let mut body = pin!(EncodeBody::new_client(encoder, source, None, None));
    let waker = std::task::Waker::noop();
    let mut cx = Context::from_waker(waker);

    let frame = match body.as_mut().poll_frame(&mut cx) {
        Poll::Ready(Some(Ok(frame))) => frame,
        Poll::Ready(Some(Err(status))) => panic!("encode failed: {status}"),
        Poll::Ready(None) => panic!("body ended before data"),
        Poll::Pending => panic!("synchronous buf encode should not be pending"),
    };

    frame.into_data().expect("got data frame")
}

fn dsa_encoder() -> DsaSyncBufEncoder {
    let path = std::env::var_os(DSA_WQ_ENV).unwrap_or_else(|| {
        panic!("set {DSA_WQ_ENV} to an idxd work-queue path, for example /dev/dsa/wq0.0")
    });
    let config = DsaConfig::new(path).with_min_message_bytes(1);
    let work_queue = DsaWorkQueue::open(config).expect("open DSA work queue");
    DsaSyncBufEncoder::with_work_queue(BufferSettings::default(), work_queue)
}

macro_rules! bench_software {
    ($name:ident, $payload_len:expr) => {
        fn $name(b: &mut Bencher) {
            let payload = make_payload($payload_len);
            b.bytes = payload.len() as u64;

            b.iter(|| {
                let encoded = encode_one(SoftwareBufEncoder, payload.clone());
                black_box(encoded);
            });
        }
    };
}

macro_rules! bench_dsa {
    ($name:ident, $payload_len:expr) => {
        fn $name(b: &mut Bencher) {
            let payload = make_payload($payload_len);
            let encoder = dsa_encoder();
            b.bytes = payload.len() as u64;

            b.iter(|| {
                let encoded = encode_one(encoder.clone(), payload.clone());
                black_box(encoded);
            });
        }
    };
}

bench_software!(software_4k, 4 * 1024);
bench_software!(software_64k, 64 * 1024);
bench_software!(software_1m, 1024 * 1024);

bench_dsa!(dsa_4k, 4 * 1024);
bench_dsa!(dsa_64k, 64 * 1024);
bench_dsa!(dsa_1m, 1024 * 1024);

benchmark_group!(software, software_4k, software_64k, software_1m);
benchmark_group!(dsa, dsa_4k, dsa_64k, dsa_1m);
benchmark_main!(software, dsa);
