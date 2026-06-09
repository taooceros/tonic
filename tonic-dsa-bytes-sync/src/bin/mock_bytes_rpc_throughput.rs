#![allow(missing_docs)]

use bytes::{Buf, BufMut, Bytes};
use http_body::Body as HttpBody;
use std::{
    convert::Infallible,
    env,
    ffi::OsString,
    future::{Future, Ready, ready},
    net::SocketAddr,
    pin::Pin,
    task::{Context, Poll},
    time::{Duration, Instant},
};
use tokio::{sync::oneshot, task::JoinSet};
use tonic::{
    Request, Response, Status,
    body::Body,
    client::Grpc,
    codec::{BufferSettings, Codec, DecodeBuf, Decoder, EncodeBuffer, Encoder, ReadyEncode},
    server::{NamedService, UnaryService},
    transport::{Channel, Endpoint, Server, server::TcpIncoming},
};
use tonic_dsa_bytes_sync::{DsaBufCodec, DsaConfig, DsaWorkQueue};
use tower_service::Service;

const DSA_WQ_ENV: &str = "TONIC_DSA_WQ";
const ECHO_PATH: &str = "/benchmark.Bytes/Echo";
const SERVICE_NAME: &str = "benchmark.Bytes";
const DEFAULT_REQUESTS: usize = 1_000;
const DEFAULT_WARMUP: usize = 32;
const DEFAULT_CONCURRENCY: usize = 64;
const DEFAULT_DSA_MIN_MESSAGE_BYTES: usize = 1;

const DEFAULT_PAYLOADS: &[usize] = &[4 * 1024, 64 * 1024, 1024 * 1024];

type BoxBuf = Box<dyn Buf + Send + Sync>;
type BoxFuture<T, E> = Pin<Box<dyn Future<Output = Result<T, E>> + Send + 'static>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CodecChoice {
    Standard,
    Dsa,
    Both,
}

#[derive(Debug, Clone)]
struct Config {
    codec: CodecChoice,
    payload_bytes: Vec<usize>,
    requests: usize,
    concurrency: usize,
    warmup: usize,
    dsa_wq: Option<OsString>,
    dsa_min_message_bytes: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            codec: CodecChoice::Both,
            payload_bytes: DEFAULT_PAYLOADS.to_vec(),
            requests: DEFAULT_REQUESTS,
            concurrency: DEFAULT_CONCURRENCY,
            warmup: DEFAULT_WARMUP,
            dsa_wq: env::var_os(DSA_WQ_ENV),
            dsa_min_message_bytes: DEFAULT_DSA_MIN_MESSAGE_BYTES,
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct StandardBytesCodec;

#[derive(Debug, Clone, Copy, Default)]
struct StandardBytesEncoder;

#[derive(Debug, Clone, Copy, Default)]
struct StandardBytesDecoder;

#[derive(Debug, Clone)]
struct BytesEchoServer<C> {
    codec: C,
}

#[derive(Debug)]
struct RunningServer {
    addr: SocketAddr,
    shutdown: oneshot::Sender<()>,
}

impl RunningServer {
    fn shutdown(self) {
        let _ = self.shutdown.send(());
    }
}

#[derive(Debug, Clone, Copy)]
struct EchoUnary;

#[derive(Debug, Clone, Copy)]
struct Measurement {
    elapsed: Duration,
    requests: usize,
    payload_bytes: usize,
    concurrency: usize,
}

impl Measurement {
    fn requests_per_second(self) -> f64 {
        self.requests as f64 / self.elapsed.as_secs_f64()
    }

    fn one_way_mib_per_second(self) -> f64 {
        let bytes = self.requests * self.payload_bytes;
        bytes as f64 / self.elapsed.as_secs_f64() / (1024.0 * 1024.0)
    }

    fn round_trip_mib_per_second(self) -> f64 {
        self.one_way_mib_per_second() * 2.0
    }
}

impl Codec for StandardBytesCodec {
    type Encode = BoxBuf;
    type Decode = Bytes;
    type Encoder = StandardBytesEncoder;
    type Decoder = StandardBytesDecoder;

    fn encoder(&mut self) -> Self::Encoder {
        StandardBytesEncoder
    }

    fn decoder(&mut self) -> Self::Decoder {
        StandardBytesDecoder
    }
}

impl Encoder for StandardBytesEncoder {
    type Item = BoxBuf;
    type Error = Status;
    type Encode = ReadyEncode<Status>;

    #[inline]
    fn encode(
        self: Pin<&mut Self>,
        mut item: Self::Item,
        mut dst: EncodeBuffer,
    ) -> Result<Self::Encode, Status> {
        let _ = self;
        dst.as_encode_buf().put(&mut *item);
        Ok(ReadyEncode::new(dst))
    }

    #[inline]
    fn buffer_settings(&self) -> BufferSettings {
        BufferSettings::default()
    }
}

impl Decoder for StandardBytesDecoder {
    type Item = Bytes;
    type Error = Status;
    type Decode = Ready<Result<Option<Bytes>, Status>>;

    #[inline]
    fn decode(self: Pin<&mut Self>, mut src: DecodeBuf<'_>) -> Result<Self::Decode, Status> {
        let _ = self;
        Ok(ready(Ok(Some(src.copy_to_bytes(src.remaining())))))
    }

    #[inline]
    fn buffer_settings(&self) -> BufferSettings {
        BufferSettings::default()
    }
}

impl<C> BytesEchoServer<C> {
    fn new(codec: C) -> Self {
        Self { codec }
    }
}

impl<C, B> Service<http::Request<B>> for BytesEchoServer<C>
where
    C: Codec<Encode = BoxBuf, Decode = Bytes> + Clone + Send + 'static,
    C::Encoder: Send + 'static,
    C::Decoder: Send + 'static,
    B: HttpBody + Send + 'static,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>> + Send,
{
    type Response = http::Response<Body>;
    type Error = Infallible;
    type Future = BoxFuture<Self::Response, Self::Error>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: http::Request<B>) -> Self::Future {
        if req.uri().path() != ECHO_PATH {
            return Box::pin(async move {
                let mut response = http::Response::new(Body::default());
                let headers = response.headers_mut();
                headers.insert(
                    tonic::Status::GRPC_STATUS,
                    (tonic::Code::Unimplemented as i32).into(),
                );
                headers.insert(
                    http::header::CONTENT_TYPE,
                    tonic::metadata::GRPC_CONTENT_TYPE,
                );
                Ok(response)
            });
        }

        let codec = self.codec.clone();
        Box::pin(async move {
            let mut grpc = tonic::server::Grpc::new(codec);
            Ok(grpc.unary(EchoUnary, req).await)
        })
    }
}

impl<C> NamedService for BytesEchoServer<C> {
    const NAME: &'static str = SERVICE_NAME;
}

impl UnaryService<Bytes> for EchoUnary {
    type Response = BoxBuf;
    type Future = BoxFuture<Response<Self::Response>, Status>;

    fn call(&mut self, request: Request<Bytes>) -> Self::Future {
        Box::pin(async move { Ok(Response::new(Box::new(request.into_inner()) as BoxBuf)) })
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = parse_args(env::args_os().skip(1))?;

    println!(
        "codec,payload_bytes,requests,concurrency,elapsed_ms,requests_per_s,one_way_mib_per_s,round_trip_mib_per_s"
    );

    if matches!(config.codec, CodecChoice::Standard | CodecChoice::Both) {
        for &payload_bytes in &config.payload_bytes {
            run_and_print(
                "standard",
                payload_bytes,
                config.requests,
                config.concurrency,
                config.warmup,
                StandardBytesCodec,
            )
            .await?;
        }
    }

    if matches!(config.codec, CodecChoice::Dsa | CodecChoice::Both) {
        let Some(dsa_wq) = config.dsa_wq.as_ref() else {
            eprintln!("skipping dsa: set {DSA_WQ_ENV} or pass --dsa-wq /dev/dsa/wqX.Y");
            return Ok(());
        };

        for &payload_bytes in &config.payload_bytes {
            let codec = dsa_codec(dsa_wq.clone(), config.dsa_min_message_bytes)?;
            run_and_print(
                "dsa",
                payload_bytes,
                config.requests,
                config.concurrency,
                config.warmup,
                codec,
            )
            .await?;
        }
    }

    Ok(())
}

async fn run_and_print<C>(
    label: &str,
    payload_bytes: usize,
    requests: usize,
    concurrency: usize,
    warmup: usize,
    codec: C,
) -> Result<(), Box<dyn std::error::Error>>
where
    C: Codec<Encode = BoxBuf, Decode = Bytes> + Clone + Send + Sync + 'static,
    C::Encoder: Send + 'static,
    C::Decoder: Send + 'static,
{
    let measurement = measure_codec(payload_bytes, requests, concurrency, warmup, codec).await?;
    println!(
        "{label},{payload_bytes},{requests},{},{:.3},{:.2},{:.2},{:.2}",
        measurement.concurrency,
        measurement.elapsed.as_secs_f64() * 1_000.0,
        measurement.requests_per_second(),
        measurement.one_way_mib_per_second(),
        measurement.round_trip_mib_per_second(),
    );
    Ok(())
}

async fn measure_codec<C>(
    payload_bytes: usize,
    requests: usize,
    concurrency: usize,
    warmup: usize,
    codec: C,
) -> Result<Measurement, Box<dyn std::error::Error>>
where
    C: Codec<Encode = BoxBuf, Decode = Bytes> + Clone + Send + Sync + 'static,
    C::Encoder: Send + 'static,
    C::Decoder: Send + 'static,
{
    let request = payload(payload_bytes);
    let server = spawn_server(codec.clone()).await?;
    let channel = connect_channel(server.addr).await?;
    let mut client = Grpc::new(channel.clone());

    let echoed = unary_echo(&mut client, codec.clone(), request.clone()).await?;
    assert_eq!(echoed, request, "benchmark echo path changed payload bytes");

    drive_concurrent_requests(
        channel.clone(),
        codec.clone(),
        request.clone(),
        warmup,
        concurrency,
    )
    .await?;

    let started = Instant::now();
    drive_concurrent_requests(channel, codec, request, requests, concurrency).await?;
    let elapsed = started.elapsed();
    server.shutdown();

    Ok(Measurement {
        elapsed,
        requests,
        payload_bytes,
        concurrency: concurrency.min(requests).max(1),
    })
}

fn payload(len: usize) -> Bytes {
    Bytes::from(vec![0x5a; len])
}

fn dsa_codec(
    path: OsString,
    min_message_bytes: usize,
) -> Result<DsaBufCodec, Box<dyn std::error::Error>> {
    let config = DsaConfig::new(path).with_min_message_bytes(min_message_bytes);
    let work_queue = DsaWorkQueue::open(config)?;
    Ok(DsaBufCodec::with_work_queue(work_queue))
}

async fn spawn_server<C>(codec: C) -> Result<RunningServer, Box<dyn std::error::Error>>
where
    C: Codec<Encode = BoxBuf, Decode = Bytes> + Clone + Send + Sync + 'static,
    C::Encoder: Send + 'static,
    C::Decoder: Send + 'static,
{
    let incoming = TcpIncoming::bind("127.0.0.1:0".parse()?)?.with_nodelay(Some(true));
    let addr = incoming.local_addr()?;
    let service = BytesEchoServer::new(codec);
    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    tokio::spawn(async move {
        Server::builder()
            .add_service(service)
            .serve_with_incoming_shutdown(incoming, async {
                let _ = shutdown_rx.await;
            })
            .await
            .expect("serve benchmark echo service");
    });

    Ok(RunningServer {
        addr,
        shutdown: shutdown_tx,
    })
}

async fn connect_channel(addr: SocketAddr) -> Result<Channel, Box<dyn std::error::Error>> {
    let endpoint = Endpoint::from_shared(format!("http://{addr}"))?.tcp_nodelay(true);
    Ok(endpoint.connect().await?)
}

async fn drive_concurrent_requests<C>(
    channel: Channel,
    codec: C,
    request: Bytes,
    total_requests: usize,
    concurrency: usize,
) -> Result<(), Box<dyn std::error::Error>>
where
    C: Codec<Encode = BoxBuf, Decode = Bytes> + Clone + Send + Sync + 'static,
    C::Encoder: Send + 'static,
    C::Decoder: Send + 'static,
{
    if total_requests == 0 {
        return Ok(());
    }

    let workers = concurrency.min(total_requests).max(1);
    let base_requests = total_requests / workers;
    let extra_requests = total_requests % workers;
    let payload_bytes = request.len();
    let mut tasks = JoinSet::new();

    for worker_index in 0..workers {
        let worker_requests = base_requests + usize::from(worker_index < extra_requests);
        let mut client = Grpc::new(channel.clone());
        let codec = codec.clone();
        let request = request.clone();

        tasks.spawn(async move {
            for _ in 0..worker_requests {
                let echoed = unary_echo(&mut client, codec.clone(), request.clone()).await?;
                if echoed.len() != payload_bytes {
                    return Err(Status::internal(format!(
                        "benchmark echo returned {} bytes, expected {payload_bytes}",
                        echoed.len()
                    )));
                }
            }
            Ok::<(), Status>(())
        });
    }

    while let Some(result) = tasks.join_next().await {
        result
            .map_err(|err| std::io::Error::other(format!("throughput worker failed: {err}")))?
            .map_err(|status| std::io::Error::other(format!("throughput RPC failed: {status}")))?;
    }

    Ok(())
}

async fn unary_echo<C>(
    client: &mut Grpc<Channel>,
    codec: C,
    request: Bytes,
) -> Result<Bytes, Status>
where
    C: Codec<Encode = BoxBuf, Decode = Bytes>,
    C::Encoder: Send + 'static,
    C::Decoder: Send + 'static,
{
    client
        .ready()
        .await
        .map_err(|err| Status::unknown(format!("benchmark client not ready: {err}")))?;
    let response = client
        .unary(
            Request::new(Box::new(request) as BoxBuf),
            http::uri::PathAndQuery::from_static(ECHO_PATH),
            codec,
        )
        .await?;
    Ok(response.into_inner())
}

fn parse_args(
    args: impl IntoIterator<Item = OsString>,
) -> Result<Config, Box<dyn std::error::Error>> {
    let mut config = Config::default();
    let mut payloads = Vec::new();
    let mut args = args.into_iter();

    while let Some(arg) = args.next() {
        match arg.to_str() {
            Some("--codec") => {
                let value = next_value("--codec", &mut args)?;
                config.codec = match value.to_str() {
                    Some("standard") => CodecChoice::Standard,
                    Some("dsa") => CodecChoice::Dsa,
                    Some("both") => CodecChoice::Both,
                    _ => return Err("--codec must be standard, dsa, or both".into()),
                };
            }
            Some("--payload-bytes") => {
                let value = next_value("--payload-bytes", &mut args)?;
                payloads.push(parse_usize("--payload-bytes", &value)?);
            }
            Some("--requests") => {
                let value = next_value("--requests", &mut args)?;
                config.requests = parse_usize("--requests", &value)?;
            }
            Some("--concurrency") => {
                let value = next_value("--concurrency", &mut args)?;
                config.concurrency = parse_usize("--concurrency", &value)?;
            }
            Some("--warmup") => {
                let value = next_value("--warmup", &mut args)?;
                config.warmup = parse_usize("--warmup", &value)?;
            }
            Some("--dsa-wq") => {
                config.dsa_wq = Some(next_value("--dsa-wq", &mut args)?);
            }
            Some("--dsa-min-message-bytes") => {
                let value = next_value("--dsa-min-message-bytes", &mut args)?;
                config.dsa_min_message_bytes = parse_usize("--dsa-min-message-bytes", &value)?;
            }
            Some("--help" | "-h") => {
                print_usage();
                std::process::exit(0);
            }
            _ => return Err(format!("unknown argument: {}", arg.to_string_lossy()).into()),
        }
    }

    if !payloads.is_empty() {
        config.payload_bytes = payloads;
    }
    if config.requests == 0 {
        return Err("--requests must be greater than zero".into());
    }
    if config.concurrency == 0 {
        return Err("--concurrency must be greater than zero".into());
    }

    Ok(config)
}

fn next_value(
    name: &'static str,
    args: &mut impl Iterator<Item = OsString>,
) -> Result<OsString, Box<dyn std::error::Error>> {
    args.next()
        .ok_or_else(|| format!("{name} requires a value").into())
}

fn parse_usize(name: &'static str, value: &OsString) -> Result<usize, Box<dyn std::error::Error>> {
    value
        .to_str()
        .ok_or_else(|| format!("{name} must be valid UTF-8"))?
        .parse()
        .map_err(|_| format!("{name} must be a positive integer").into())
}

fn print_usage() {
    println!(
        "Usage: cargo run -p tonic-dsa-bytes-sync --bin mock_bytes_rpc_throughput -- \
    [--codec standard|dsa|both] [--payload-bytes N ...] [--requests N] [--concurrency N] [--warmup N] [--dsa-wq PATH] [--dsa-min-message-bytes N]\n\n\
Defaults: --codec both --payload-bytes 4096 --payload-bytes 65536 --payload-bytes 1048576 \
--requests {DEFAULT_REQUESTS} --concurrency {DEFAULT_CONCURRENCY} --warmup {DEFAULT_WARMUP} --dsa-min-message-bytes {DEFAULT_DSA_MIN_MESSAGE_BYTES}\n\n\
DSA runs require launch plus --dsa-wq PATH or {DSA_WQ_ENV}=PATH. Payloads smaller than --dsa-min-message-bytes use the CPU encode path. Output is CSV."
    );
}
