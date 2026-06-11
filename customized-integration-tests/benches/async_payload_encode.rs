use criterion::profiler::Profiler;
use criterion::{criterion_group, criterion_main, Criterion};
use prost::transfer::{AsyncEncodeRefExt as _, BufMutEncodeTarget};
use prost::Message as _;
use std::future::Future;
use std::hint::black_box;
use std::path::Path;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

#[cfg(feature = "hardware-dsa")]
use {
    idxd_rust::{detect_wq_mode, DsaCompletionRecord, DsaCompletionStatus, DsaHwDesc, WqPortal},
    prost::transfer::{AsyncEncodeTarget, EncodePayload},
    std::{
        env,
        marker::PhantomPinned,
        path::PathBuf,
        pin::Pin,
        sync::atomic::{AtomicBool, AtomicU64, Ordering},
    },
};

pub mod generated {
    include!(concat!(env!("OUT_DIR"), "/customized.integration.bench.rs"));
}

use generated::{NestedPayload, PayloadShape};

const PAYLOAD_CORPUS_BYTES: usize = 1024 * 1024 * 1024;
const THREAD_COUNTS: &[usize] = &[1, 2, 4, 8, 16];
const BATCH_TARGET_BYTES: usize = 16 * 1024 * 1024;

#[derive(Default)]
struct DsaSubmissionProfiler;

impl Profiler for DsaSubmissionProfiler {
    fn start_profiling(&mut self, _benchmark_id: &str, _benchmark_dir: &Path) {
        #[cfg(feature = "hardware-dsa")]
        {
            DSA_SUBMISSION_PROFILE_ACTIVE.store(false, Ordering::Release);
            DSA_SUBMISSION_PROFILE.reset();
            DSA_SUBMISSION_PROFILE_ACTIVE.store(true, Ordering::Release);
        }
    }

    fn stop_profiling(&mut self, benchmark_id: &str, benchmark_dir: &Path) {
        #[cfg(feature = "hardware-dsa")]
        {
            DSA_SUBMISSION_PROFILE_ACTIVE.store(false, Ordering::Release);

            let snapshot = DSA_SUBMISSION_PROFILE.snapshot();
            if !snapshot.has_data() {
                return;
            }

            let report = snapshot.format_report(benchmark_id);
            eprintln!("{report}");

            let path = benchmark_dir.join("dsa-submission-profile.txt");
            if let Err(error) = std::fs::create_dir_all(benchmark_dir) {
                eprintln!(
                    "failed to create DSA submission profile directory {}: {error}",
                    benchmark_dir.display()
                );
                return;
            }
            if let Err(error) = std::fs::write(&path, report.as_bytes()) {
                eprintln!(
                    "failed to write DSA submission profile to {}: {error}",
                    path.display()
                );
            }
        }

        #[cfg(not(feature = "hardware-dsa"))]
        {
            let _ = (benchmark_id, benchmark_dir);
        }
    }
}

#[cfg(feature = "hardware-dsa")]
static DSA_SUBMISSION_PROFILE_ACTIVE: AtomicBool = AtomicBool::new(false);

#[cfg(feature = "hardware-dsa")]
static DSA_SUBMISSION_PROFILE: DsaSubmissionProfileCounters = DsaSubmissionProfileCounters::new();

#[cfg(feature = "hardware-dsa")]
struct DsaSubmissionProfileCounters {
    payload_count: AtomicU64,
    payload_bytes: AtomicU64,
    start_payload_copy_ns: AtomicU64,
    touch_pages_ns: AtomicU64,
    portal_submit_ns: AtomicU64,
    batch_poll_loop_ns: AtomicU64,
    batch_count: AtomicU64,
    poll_round_count: AtomicU64,
    future_poll_count: AtomicU64,
    future_pending_count: AtomicU64,
    future_ready_count: AtomicU64,
    completion_poll_ns: AtomicU64,
    completion_poll_count: AtomicU64,
    completion_pending_count: AtomicU64,
    completion_ready_count: AtomicU64,
}

#[cfg(feature = "hardware-dsa")]
impl DsaSubmissionProfileCounters {
    const fn new() -> Self {
        Self {
            payload_count: AtomicU64::new(0),
            payload_bytes: AtomicU64::new(0),
            start_payload_copy_ns: AtomicU64::new(0),
            touch_pages_ns: AtomicU64::new(0),
            portal_submit_ns: AtomicU64::new(0),
            batch_poll_loop_ns: AtomicU64::new(0),
            batch_count: AtomicU64::new(0),
            poll_round_count: AtomicU64::new(0),
            future_poll_count: AtomicU64::new(0),
            future_pending_count: AtomicU64::new(0),
            future_ready_count: AtomicU64::new(0),
            completion_poll_ns: AtomicU64::new(0),
            completion_poll_count: AtomicU64::new(0),
            completion_pending_count: AtomicU64::new(0),
            completion_ready_count: AtomicU64::new(0),
        }
    }

    fn reset(&self) {
        self.payload_count.store(0, Ordering::Relaxed);
        self.payload_bytes.store(0, Ordering::Relaxed);
        self.start_payload_copy_ns.store(0, Ordering::Relaxed);
        self.touch_pages_ns.store(0, Ordering::Relaxed);
        self.portal_submit_ns.store(0, Ordering::Relaxed);
        self.batch_poll_loop_ns.store(0, Ordering::Relaxed);
        self.batch_count.store(0, Ordering::Relaxed);
        self.poll_round_count.store(0, Ordering::Relaxed);
        self.future_poll_count.store(0, Ordering::Relaxed);
        self.future_pending_count.store(0, Ordering::Relaxed);
        self.future_ready_count.store(0, Ordering::Relaxed);
        self.completion_poll_ns.store(0, Ordering::Relaxed);
        self.completion_poll_count.store(0, Ordering::Relaxed);
        self.completion_pending_count.store(0, Ordering::Relaxed);
        self.completion_ready_count.store(0, Ordering::Relaxed);
    }

    fn snapshot(&self) -> DsaSubmissionProfileSnapshot {
        DsaSubmissionProfileSnapshot {
            payload_count: self.payload_count.load(Ordering::Relaxed),
            payload_bytes: self.payload_bytes.load(Ordering::Relaxed),
            start_payload_copy_ns: self.start_payload_copy_ns.load(Ordering::Relaxed),
            touch_pages_ns: self.touch_pages_ns.load(Ordering::Relaxed),
            portal_submit_ns: self.portal_submit_ns.load(Ordering::Relaxed),
            batch_poll_loop_ns: self.batch_poll_loop_ns.load(Ordering::Relaxed),
            batch_count: self.batch_count.load(Ordering::Relaxed),
            poll_round_count: self.poll_round_count.load(Ordering::Relaxed),
            future_poll_count: self.future_poll_count.load(Ordering::Relaxed),
            future_pending_count: self.future_pending_count.load(Ordering::Relaxed),
            future_ready_count: self.future_ready_count.load(Ordering::Relaxed),
            completion_poll_ns: self.completion_poll_ns.load(Ordering::Relaxed),
            completion_poll_count: self.completion_poll_count.load(Ordering::Relaxed),
            completion_pending_count: self.completion_pending_count.load(Ordering::Relaxed),
            completion_ready_count: self.completion_ready_count.load(Ordering::Relaxed),
        }
    }
}

#[cfg(feature = "hardware-dsa")]
struct DsaSubmissionProfileSnapshot {
    payload_count: u64,
    payload_bytes: u64,
    start_payload_copy_ns: u64,
    touch_pages_ns: u64,
    portal_submit_ns: u64,
    batch_poll_loop_ns: u64,
    batch_count: u64,
    poll_round_count: u64,
    future_poll_count: u64,
    future_pending_count: u64,
    future_ready_count: u64,
    completion_poll_ns: u64,
    completion_poll_count: u64,
    completion_pending_count: u64,
    completion_ready_count: u64,
}

#[cfg(feature = "hardware-dsa")]
impl DsaSubmissionProfileSnapshot {
    fn has_data(&self) -> bool {
        self.payload_count != 0 || self.future_poll_count != 0 || self.completion_poll_count != 0
    }

    fn format_report(&self, benchmark_id: &str) -> String {
        format!(
            "\
DSA submission profile: {benchmark_id}
  payloads: {payload_count}
  payload bytes: {payload_bytes}
  start_payload_copy: {start_ms:.3} ms ({start_per_payload:.1} ns/payload)
    touch pages: {touch_ms:.3} ms ({touch_per_payload:.1} ns/payload, {touch_share:.1}% of start)
    portal submit: {submit_ms:.3} ms ({submit_per_payload:.1} ns/payload, {submit_share:.1}% of start)
  batch poll loop: {poll_loop_ms:.3} ms ({poll_loop_per_payload:.1} ns/payload)
    batches/poll rounds: {batches}/{poll_rounds}
    future polls: {future_polls} pending={future_pending} ready={future_ready}
  completion status polls: {completion_polls} pending={completion_pending} ready={completion_ready}
    status read time: {completion_ms:.3} ms ({completion_per_poll:.1} ns/poll)
",
            benchmark_id = benchmark_id,
            payload_count = self.payload_count,
            payload_bytes = self.payload_bytes,
            start_ms = ns_to_ms(self.start_payload_copy_ns),
            start_per_payload = per_count(self.start_payload_copy_ns, self.payload_count),
            touch_ms = ns_to_ms(self.touch_pages_ns),
            touch_per_payload = per_count(self.touch_pages_ns, self.payload_count),
            touch_share = percent(self.touch_pages_ns, self.start_payload_copy_ns),
            submit_ms = ns_to_ms(self.portal_submit_ns),
            submit_per_payload = per_count(self.portal_submit_ns, self.payload_count),
            submit_share = percent(self.portal_submit_ns, self.start_payload_copy_ns),
            poll_loop_ms = ns_to_ms(self.batch_poll_loop_ns),
            poll_loop_per_payload = per_count(self.batch_poll_loop_ns, self.payload_count),
            batches = self.batch_count,
            poll_rounds = self.poll_round_count,
            future_polls = self.future_poll_count,
            future_pending = self.future_pending_count,
            future_ready = self.future_ready_count,
            completion_polls = self.completion_poll_count,
            completion_pending = self.completion_pending_count,
            completion_ready = self.completion_ready_count,
            completion_ms = ns_to_ms(self.completion_poll_ns),
            completion_per_poll = per_count(self.completion_poll_ns, self.completion_poll_count),
        )
    }
}

#[cfg(feature = "hardware-dsa")]
fn dsa_profile_active() -> bool {
    DSA_SUBMISSION_PROFILE_ACTIVE.load(Ordering::Acquire)
}

#[cfg(feature = "hardware-dsa")]
fn dsa_profile_start(active: bool) -> Option<Instant> {
    if active {
        Some(Instant::now())
    } else {
        None
    }
}

#[cfg(feature = "hardware-dsa")]
fn dsa_profile_record_payload_copy(start: Option<Instant>, bytes: usize) {
    let Some(start) = start else {
        return;
    };

    DSA_SUBMISSION_PROFILE
        .payload_count
        .fetch_add(1, Ordering::Relaxed);
    DSA_SUBMISSION_PROFILE
        .payload_bytes
        .fetch_add(u64::try_from(bytes).unwrap_or(u64::MAX), Ordering::Relaxed);
    DSA_SUBMISSION_PROFILE
        .start_payload_copy_ns
        .fetch_add(elapsed_ns(start), Ordering::Relaxed);
}

#[cfg(feature = "hardware-dsa")]
fn dsa_profile_record_touch_pages(start: Option<Instant>) {
    dsa_profile_record_duration(&DSA_SUBMISSION_PROFILE.touch_pages_ns, start);
}

#[cfg(feature = "hardware-dsa")]
fn dsa_profile_record_portal_submit(start: Option<Instant>) {
    dsa_profile_record_duration(&DSA_SUBMISSION_PROFILE.portal_submit_ns, start);
}

#[cfg(feature = "hardware-dsa")]
fn dsa_profile_record_completion_poll(start: Option<Instant>, pending: bool) {
    let Some(start) = start else {
        return;
    };

    DSA_SUBMISSION_PROFILE
        .completion_poll_count
        .fetch_add(1, Ordering::Relaxed);
    if pending {
        DSA_SUBMISSION_PROFILE
            .completion_pending_count
            .fetch_add(1, Ordering::Relaxed);
    } else {
        DSA_SUBMISSION_PROFILE
            .completion_ready_count
            .fetch_add(1, Ordering::Relaxed);
    }
    DSA_SUBMISSION_PROFILE
        .completion_poll_ns
        .fetch_add(elapsed_ns(start), Ordering::Relaxed);
}

#[cfg(feature = "hardware-dsa")]
fn dsa_profile_record_batch_poll_loop(
    start: Option<Instant>,
    poll_rounds: u64,
    future_polls: u64,
    future_pending: u64,
    future_ready: u64,
) {
    let Some(start) = start else {
        return;
    };

    DSA_SUBMISSION_PROFILE
        .batch_poll_loop_ns
        .fetch_add(elapsed_ns(start), Ordering::Relaxed);

    DSA_SUBMISSION_PROFILE
        .batch_count
        .fetch_add(1, Ordering::Relaxed);
    DSA_SUBMISSION_PROFILE
        .poll_round_count
        .fetch_add(poll_rounds, Ordering::Relaxed);
    DSA_SUBMISSION_PROFILE
        .future_poll_count
        .fetch_add(future_polls, Ordering::Relaxed);
    DSA_SUBMISSION_PROFILE
        .future_pending_count
        .fetch_add(future_pending, Ordering::Relaxed);
    DSA_SUBMISSION_PROFILE
        .future_ready_count
        .fetch_add(future_ready, Ordering::Relaxed);
}

#[cfg(feature = "hardware-dsa")]
fn dsa_profile_record_duration(counter: &AtomicU64, start: Option<Instant>) {
    if let Some(start) = start {
        counter.fetch_add(elapsed_ns(start), Ordering::Relaxed);
    }
}

#[cfg(feature = "hardware-dsa")]
fn elapsed_ns(start: Instant) -> u64 {
    u64::try_from(start.elapsed().as_nanos()).unwrap_or(u64::MAX)
}

#[cfg(feature = "hardware-dsa")]
fn ns_to_ms(ns: u64) -> f64 {
    ns as f64 / 1_000_000.0
}

#[cfg(feature = "hardware-dsa")]
fn per_count(total: u64, count: u64) -> f64 {
    if count == 0 {
        0.0
    } else {
        total as f64 / count as f64
    }
}

#[cfg(feature = "hardware-dsa")]
fn percent(part: u64, total: u64) -> f64 {
    if total == 0 {
        0.0
    } else {
        part as f64 * 100.0 / total as f64
    }
}
#[cfg(feature = "hardware-dsa")]
const PAGE_SIZE: usize = 4096;

fn benchmark_async_payload_encode(criterion: &mut Criterion) {
    let cases = payload_cases();
    verify_cpu_paths(&cases);

    let mut group = criterion.benchmark_group("prost_async_payload_encode");
    for case in &cases {
        let encoded_len = case.message.encoded_len();
        let payloads = payload_count(&case.message);
        let requests_per_iter = requests_per_iteration(encoded_len);

        group.bench_function(
            format!(
                "{} / CPU Prost Message::encode / requests={} bytes_per_request={} bytes_per_iter={} payloads_per_request={}",
                case.name,
                requests_per_iter,
                encoded_len,
                requests_per_iter * encoded_len,
                payloads
            ),
            |b| {
                let corpus = payload_corpus(&case.message);
                let mut outputs = output_corpus(corpus.len(), encoded_len);
                let mut request_index = 0;
                b.iter(|| {
                    let mut encoded_bytes = 0;
                    for _ in 0..requests_per_iter {
                        let index = next_payload_index(&corpus, &mut request_index);
                        let output = &mut outputs[index];
                        output.clear();
                        corpus[index]
                            .encode(output)
                            .expect("CPU Prost encode succeeds");
                        encoded_bytes += output.len();
                    }
                    black_box(encoded_bytes);
                });
            },
        );

        group.bench_function(
            format!(
                "{} / async CPU Prost encode_async_ref / requests={} bytes_per_request={} bytes_per_iter={} payloads_per_request={}",
                case.name,
                requests_per_iter,
                encoded_len,
                requests_per_iter * encoded_len,
                payloads
            ),
            |b| {
                let corpus = payload_corpus(&case.message);
                let mut outputs = output_corpus(corpus.len(), encoded_len);
                let mut request_index = 0;
                b.iter(|| {
                    let mut encoded_bytes = 0;
                    for _ in 0..requests_per_iter {
                        let index = next_payload_index(&corpus, &mut request_index);
                        let output = &mut outputs[index];
                        output.clear();
                        let mut target = BufMutEncodeTarget::new(output);
                        poll_until_ready(corpus[index].encode_async_ref(&mut target))
                            .expect("async CPU Prost encode succeeds");
                        encoded_bytes += output.len();
                    }
                    black_box(encoded_bytes);
                });
            },
        );
    }
    group.finish();

    benchmark_cpu_thread_matrix(criterion, &cases);

    benchmark_dsa_paths(criterion, &cases);
}

fn benchmark_cpu_thread_matrix(criterion: &mut Criterion, cases: &[PayloadCase]) {
    let mut group = criterion.benchmark_group("prost_async_payload_encode_cpu_thread_matrix");
    for case in cases {
        let encoded_len = case.message.encoded_len();
        let payloads = payload_count(&case.message);
        let requests_per_iter = requests_per_iteration(encoded_len);
        for &thread_count in THREAD_COUNTS {
            group.bench_function(
                format!(
                    "{} / CPU Prost thread matrix / threads={} requests_per_iter={} bytes_per_request={} bytes_per_iter={} payloads_per_request={}",
                    case.name,
                    thread_count,
                    requests_per_iter,
                    encoded_len,
                    requests_per_iter * encoded_len,
                    payloads
                ),
                |b| {
                    let corpus = payload_corpus(&case.message);
                    b.iter_custom(|iterations| {
                        encode_cpu_threaded_iterations(
                            &corpus,
                            encoded_len,
                            requests_per_iter,
                            thread_count,
                            iterations,
                        )
                    });
                },
            );
        }
    }
    group.finish();
}

fn encode_cpu_threaded_iterations(
    corpus: &[PayloadShape],
    encoded_len: usize,
    requests_per_iter: usize,
    thread_count: usize,
    iterations: u64,
) -> Duration {
    let start_time = Instant::now();
    let encoded_bytes = std::thread::scope(|scope| {
        let mut handles = Vec::with_capacity(thread_count);
        for thread_index in 0..thread_count {
            handles.push(scope.spawn(move || {
                let start = requests_per_iter * thread_index / thread_count;
                let end = requests_per_iter * (thread_index + 1) / thread_count;
                let mut outputs = output_corpus(end - start, encoded_len);
                let mut encoded_bytes = 0;
                for iteration in 0..iterations as usize {
                    for (request_offset, output) in outputs.iter_mut().enumerate() {
                        let index =
                            (iteration * requests_per_iter + start + request_offset) % corpus.len();
                        output.clear();
                        corpus[index]
                            .encode(output)
                            .expect("CPU Prost encode succeeds");
                        encoded_bytes += output.len();
                    }
                }
                encoded_bytes
            }));
        }

        handles
            .into_iter()
            .map(|handle| handle.join().expect("CPU encode worker panicked"))
            .sum::<usize>()
    });
    black_box(encoded_bytes);
    start_time.elapsed()
}

fn verify_cpu_paths(cases: &[PayloadCase]) {
    for case in cases {
        let mut cpu = Vec::with_capacity(case.message.encoded_len());
        case.message
            .encode(&mut cpu)
            .expect("CPU Prost encode succeeds");

        let mut async_cpu = Vec::with_capacity(case.message.encoded_len());
        let mut target = BufMutEncodeTarget::new(&mut async_cpu);
        poll_until_ready(case.message.encode_async_ref(&mut target))
            .expect("async CPU Prost encode succeeds");

        assert_eq!(
            async_cpu, cpu,
            "async CPU Prost output differs from CPU output for {}",
            case.name
        );
    }
}

#[cfg(not(feature = "hardware-dsa"))]
fn benchmark_dsa_paths(_criterion: &mut Criterion, _cases: &[PayloadCase]) {
    println!("DSA Prost encode benchmark skipped: set TONIC_DSA_WQ");
}

#[cfg(feature = "hardware-dsa")]
fn benchmark_dsa_paths(criterion: &mut Criterion, cases: &[PayloadCase]) {
    let Some(work_queue) = HardwareDsaWorkQueue::open_from_env() else {
        println!("DSA Prost encode benchmark skipped: TONIC_DSA_WQ is unset");
        return;
    };

    verify_dsa_paths(cases, &work_queue);

    let mut group = criterion.benchmark_group("prost_async_payload_encode_dsa");
    for case in cases {
        let encoded_len = case.message.encoded_len();
        let payloads = payload_count(&case.message);
        let requests_per_iter = requests_per_iteration(encoded_len);
        group.bench_function(
            format!(
                "{} / DSA Prost encode_async_ref submit+poll+complete / requests={} bytes_per_request={} bytes_per_iter={} payloads_per_request={}",
                case.name,
                requests_per_iter,
                encoded_len,
                requests_per_iter * encoded_len,
                payloads
            ),
            |b| {
                let corpus = payload_corpus(&case.message);
                let mut outputs = output_corpus(corpus.len(), encoded_len);
                let mut request_index = 0;
                b.iter(|| {
                    let mut encoded_bytes = 0;
                    let mut submitted_payloads = 0;
                    let mut completed_payloads = 0;
                    for _ in 0..requests_per_iter {
                        let index = next_payload_index(&corpus, &mut request_index);
                        let output = &mut outputs[index];
                        output.clear();
                        let (payload_submitted, payload_completed) = {
                            let mut target = DsaPayloadTarget::new(&work_queue, output);
                            poll_until_ready(corpus[index].encode_async_ref(&mut target))
                                .expect("DSA Prost encode succeeds");
                            assert_eq!(target.submitted_payloads, target.completed_payloads);
                            (target.submitted_payloads, target.completed_payloads)
                        };
                        encoded_bytes += output.len();
                        submitted_payloads += payload_submitted;
                        completed_payloads += payload_completed;
                    }
                    black_box((encoded_bytes, submitted_payloads, completed_payloads));
                });
            },
        );
    }
    group.finish();

    benchmark_concurrent_dsa_paths(criterion, cases, &work_queue);
    benchmark_dsa_thread_matrix(criterion, cases, &work_queue);
}

#[cfg(feature = "hardware-dsa")]
fn benchmark_concurrent_dsa_paths(
    criterion: &mut Criterion,
    cases: &[PayloadCase],
    work_queue: &HardwareDsaWorkQueue,
) {
    const CONCURRENT_REQUESTS: &[usize] = &[1, 2, 4, 8, 16];

    for &concurrent_requests in CONCURRENT_REQUESTS {
        verify_concurrent_dsa_paths(cases, work_queue, concurrent_requests);
    }

    let mut group = criterion.benchmark_group("prost_async_payload_encode_dsa_concurrency_matrix");
    for case in cases {
        let encoded_len = case.message.encoded_len();
        let payloads = payload_count(&case.message);
        for &concurrent_requests in CONCURRENT_REQUESTS {
            let concurrent_batches =
                concurrent_batches_per_iteration(encoded_len, concurrent_requests);
            let requests_per_iter = concurrent_batches * concurrent_requests;
            group.bench_function(
                format!(
                    "{} / DSA Prost encode concurrency matrix / concurrent_requests={} requests_per_iter={} bytes_per_request={} bytes_per_iter={} payloads_per_request={}",
                    case.name,
                    concurrent_requests,
                    requests_per_iter,
                    encoded_len,
                    requests_per_iter * encoded_len,
                    payloads
                ),
                |b| {
                    let corpus = payload_corpus(&case.message);
                    let mut outputs = output_corpus(concurrent_requests, encoded_len);
                    let mut request_index = 0;
                    b.iter(|| {
                        let mut encoded_bytes = 0;
                        let mut submitted_payloads = 0;
                        let mut completed_payloads = 0;
                        for _ in 0..concurrent_batches {
                            let (batch_bytes, batch_submitted, batch_completed) =
                                encode_concurrent_dsa_batch(
                                    work_queue,
                                    &corpus,
                                    &mut request_index,
                                    &mut outputs,
                                )
                                .expect("concurrent DSA Prost encode succeeds");
                            encoded_bytes += batch_bytes;
                            submitted_payloads += batch_submitted;
                            completed_payloads += batch_completed;
                        }
                        assert_eq!(submitted_payloads, completed_payloads);
                        black_box((encoded_bytes, submitted_payloads, completed_payloads));
                    });
                },
            );
        }
    }
    group.finish();
}

#[cfg(feature = "hardware-dsa")]
fn benchmark_dsa_thread_matrix(
    criterion: &mut Criterion,
    cases: &[PayloadCase],
    work_queue: &HardwareDsaWorkQueue,
) {
    const CONCURRENT_REQUESTS: &[usize] = &[1, 2, 4, 8, 16];

    let mut group = criterion.benchmark_group("prost_async_payload_encode_dsa_thread_matrix");
    for case in cases {
        let encoded_len = case.message.encoded_len();
        let payloads = payload_count(&case.message);
        for &thread_count in THREAD_COUNTS {
            for &concurrent_requests in CONCURRENT_REQUESTS {
                let requests_per_iter = requests_per_iteration(encoded_len);
                group.bench_function(
                    format!(
                        "{} / DSA Prost thread matrix / threads={} concurrent_requests={} requests_per_iter={} bytes_per_request={} bytes_per_iter={} payloads_per_request={}",
                        case.name,
                        thread_count,
                        concurrent_requests,
                        requests_per_iter,
                        encoded_len,
                        requests_per_iter * encoded_len,
                        payloads
                    ),
                    |b| {
                        let corpus = payload_corpus(&case.message);
                        b.iter_custom(|iterations| {
                            encode_dsa_threaded_iterations(
                                work_queue,
                                &corpus,
                                encoded_len,
                                thread_count,
                                concurrent_requests,
                                iterations,
                            )
                            .expect("threaded DSA Prost encode succeeds")
                        });
                    },
                );
            }
        }
    }
    group.finish();
}

#[cfg(feature = "hardware-dsa")]
fn encode_dsa_threaded_iterations(
    work_queue: &HardwareDsaWorkQueue,
    corpus: &[PayloadShape],
    encoded_len: usize,
    thread_count: usize,
    concurrent_requests: usize,
    iterations: u64,
) -> Result<Duration, HardwareDsaError> {
    let requests_per_iter = requests_per_iteration(encoded_len);
    let start_time = Instant::now();
    let (encoded_bytes, submitted_payloads, completed_payloads) = std::thread::scope(|scope| {
        let mut handles = Vec::with_capacity(thread_count);
        for thread_index in 0..thread_count {
            handles.push(scope.spawn(move || {
                let start = requests_per_iter * thread_index / thread_count;
                let end = requests_per_iter * (thread_index + 1) / thread_count;
                let mut outputs = output_corpus(concurrent_requests, encoded_len);
                let mut encoded_bytes = 0;
                let mut submitted_payloads = 0;
                let mut completed_payloads = 0;

                for iteration in 0..iterations as usize {
                    let mut request_index = iteration * requests_per_iter + start;
                    let request_end = iteration * requests_per_iter + end;
                    while request_index < request_end {
                        let batch_requests = concurrent_requests.min(request_end - request_index);
                        let (batch_bytes, batch_submitted, batch_completed) =
                            encode_concurrent_dsa_batch(
                                work_queue,
                                corpus,
                                &mut request_index,
                                &mut outputs[..batch_requests],
                            )?;
                        encoded_bytes += batch_bytes;
                        submitted_payloads += batch_submitted;
                        completed_payloads += batch_completed;
                    }
                }

                Ok::<_, HardwareDsaError>((encoded_bytes, submitted_payloads, completed_payloads))
            }));
        }

        let mut encoded_bytes = 0;
        let mut submitted_payloads = 0;
        let mut completed_payloads = 0;
        for handle in handles {
            let (thread_bytes, thread_submitted, thread_completed) =
                handle.join().expect("DSA encode worker panicked")?;
            encoded_bytes += thread_bytes;
            submitted_payloads += thread_submitted;
            completed_payloads += thread_completed;
        }
        Ok::<_, HardwareDsaError>((encoded_bytes, submitted_payloads, completed_payloads))
    })?;
    assert_eq!(submitted_payloads, completed_payloads);
    black_box((encoded_bytes, submitted_payloads, completed_payloads));
    Ok(start_time.elapsed())
}

#[cfg(feature = "hardware-dsa")]
fn verify_concurrent_dsa_paths(
    cases: &[PayloadCase],
    work_queue: &HardwareDsaWorkQueue,
    concurrent_requests: usize,
) {
    for case in cases {
        let corpus = payload_corpus(&case.message);
        let mut request_index = 0;
        let mut outputs = output_corpus(concurrent_requests, case.message.encoded_len());
        let (_encoded_bytes, submitted_payloads, completed_payloads) =
            encode_concurrent_dsa_batch(work_queue, &corpus, &mut request_index, &mut outputs)
                .expect("concurrent DSA Prost encode succeeds");

        assert_eq!(
            submitted_payloads, completed_payloads,
            "concurrent DSA submitted payload count differs from completed count for {}",
            case.name
        );

        for (index, output) in outputs.iter().enumerate() {
            let mut cpu = Vec::with_capacity(case.message.encoded_len());
            corpus[index]
                .encode(&mut cpu)
                .expect("CPU Prost encode succeeds");
            assert_eq!(
                output.as_slice(),
                cpu.as_slice(),
                "concurrent DSA Prost output differs from CPU output for {} request {}",
                case.name,
                index
            );
        }
    }
}

#[cfg(feature = "hardware-dsa")]
fn encode_concurrent_dsa_batch(
    work_queue: &HardwareDsaWorkQueue,
    corpus: &[PayloadShape],
    request_index: &mut usize,
    outputs: &mut [Vec<u8>],
) -> Result<(usize, usize, usize), HardwareDsaError> {
    let start_index = *request_index;
    *request_index = request_index.wrapping_add(outputs.len());
    for output in outputs.iter_mut() {
        output.clear();
    }

    let mut targets = outputs
        .iter_mut()
        .map(|output| DsaPayloadTarget::new(work_queue, output))
        .collect::<Vec<_>>();
    let mut futures = targets
        .iter_mut()
        .enumerate()
        .map(|(offset, target)| {
            let message = &corpus[(start_index + offset) % corpus.len()];
            Box::pin(message.encode_async_ref(target))
                as Pin<Box<dyn Future<Output = Result<(), HardwareDsaError>> + Send + '_>>
        })
        .collect::<Vec<_>>();

    poll_dsa_batch_until_ready(&mut futures)?;
    drop(futures);

    let submitted_payloads = targets
        .iter()
        .map(|target| target.submitted_payloads)
        .sum::<usize>();
    let completed_payloads = targets
        .iter()
        .map(|target| target.completed_payloads)
        .sum::<usize>();
    drop(targets);

    let encoded_bytes = outputs.iter().map(Vec::len).sum();
    Ok((encoded_bytes, submitted_payloads, completed_payloads))
}

#[cfg(feature = "hardware-dsa")]
fn poll_dsa_batch_until_ready(
    futures: &mut [Pin<Box<dyn Future<Output = Result<(), HardwareDsaError>> + Send + '_>>],
) -> Result<(), HardwareDsaError> {
    let waker = std::task::Waker::noop();
    let mut cx = Context::from_waker(waker);
    let mut completed = vec![false; futures.len()];
    let mut remaining = futures.len();
    let profile_start = dsa_profile_start(dsa_profile_active());
    let mut poll_rounds = 0;
    let mut future_polls = 0;
    let mut future_pending = 0;
    let mut future_ready = 0;

    while remaining != 0 {
        poll_rounds += 1;

        for (index, future) in futures.iter_mut().enumerate() {
            if completed[index] {
                continue;
            }

            future_polls += 1;
            match future.as_mut().poll(&mut cx) {
                Poll::Ready(Ok(())) => {
                    future_ready += 1;
                    completed[index] = true;
                    remaining -= 1;
                }
                Poll::Ready(Err(error)) => {
                    dsa_profile_record_batch_poll_loop(
                        profile_start,
                        poll_rounds,
                        future_polls,
                        future_pending,
                        future_ready,
                    );
                    return Err(error);
                }
                Poll::Pending => {
                    future_pending += 1;
                }
            }
        }

        if remaining != 0 {
            core::hint::spin_loop();
        }
    }

    dsa_profile_record_batch_poll_loop(
        profile_start,
        poll_rounds,
        future_polls,
        future_pending,
        future_ready,
    );

    Ok(())
}
#[cfg(feature = "hardware-dsa")]
fn verify_dsa_paths(cases: &[PayloadCase], work_queue: &HardwareDsaWorkQueue) {
    for case in cases {
        let mut cpu = Vec::with_capacity(case.message.encoded_len());
        case.message
            .encode(&mut cpu)
            .expect("CPU Prost encode succeeds");

        let mut dsa = Vec::with_capacity(case.message.encoded_len());
        let mut target = DsaPayloadTarget::new(work_queue, &mut dsa);
        poll_until_ready(case.message.encode_async_ref(&mut target))
            .expect("DSA Prost encode succeeds");

        assert_eq!(
            target.submitted_payloads, target.completed_payloads,
            "DSA submitted payload count differs from completed count for {}",
            case.name
        );
        assert!(
            target.completed_payloads > 0,
            "DSA path copied no payloads for {}",
            case.name
        );
        assert_eq!(
            dsa, cpu,
            "DSA Prost output differs from CPU output for {}",
            case.name
        );
    }
}

struct PayloadCase {
    name: &'static str,
    message: PayloadShape,
}

fn payload_cases() -> Vec<PayloadCase> {
    vec![
        PayloadCase {
            name: "tiny-bytes-128",
            message: PayloadShape {
                scalar: 8,
                sequence: 80,
                enabled: true,
                large_string: String::new(),
                large_bytes: vec![0x58; 128],
                repeated_strings: Vec::new(),
                repeated_bytes: Vec::new(),
                nested: None,
                repeated_nested: Vec::new(),
                string_map: Default::default(),
                bytes_map: Default::default(),
            },
        },
        PayloadCase {
            name: "tiny-bytes-512",
            message: PayloadShape {
                scalar: 9,
                sequence: 90,
                enabled: true,
                large_string: String::new(),
                large_bytes: vec![0x59; 512],
                repeated_strings: Vec::new(),
                repeated_bytes: Vec::new(),
                nested: None,
                repeated_nested: Vec::new(),
                string_map: Default::default(),
                bytes_map: Default::default(),
            },
        },
        PayloadCase {
            name: "tiny-bytes-1024",
            message: PayloadShape {
                scalar: 10,
                sequence: 100,
                enabled: true,
                large_string: String::new(),
                large_bytes: vec![0x5a; 1024],
                repeated_strings: Vec::new(),
                repeated_bytes: Vec::new(),
                nested: None,
                repeated_nested: Vec::new(),
                string_map: Default::default(),
                bytes_map: Default::default(),
            },
        },
        PayloadCase {
            name: "small-bytes-4k",
            message: PayloadShape {
                scalar: 11,
                sequence: 110,
                enabled: true,
                large_string: String::new(),
                large_bytes: vec![0x5a; 4 * 1024],
                repeated_strings: Vec::new(),
                repeated_bytes: Vec::new(),
                nested: None,
                repeated_nested: Vec::new(),
                string_map: Default::default(),
                bytes_map: Default::default(),
            },
        },
        PayloadCase {
            name: "small-string-4k",
            message: PayloadShape {
                scalar: 12,
                sequence: 120,
                enabled: true,
                large_string: "small string payload ".repeat(205),
                large_bytes: Vec::new(),
                repeated_strings: Vec::new(),
                repeated_bytes: Vec::new(),
                nested: None,
                repeated_nested: Vec::new(),
                string_map: Default::default(),
                bytes_map: Default::default(),
            },
        },
        PayloadCase {
            name: "repeated-small-payloads",
            message: PayloadShape {
                scalar: 13,
                sequence: 130,
                enabled: true,
                large_string: String::new(),
                large_bytes: Vec::new(),
                repeated_strings: vec![
                    "small repeated string a ".repeat(64),
                    "small repeated string b ".repeat(64),
                ],
                repeated_bytes: vec![
                    vec![0x21; 2 * 1024],
                    vec![0x22; 2 * 1024],
                    vec![0x23; 2 * 1024],
                ],
                nested: None,
                repeated_nested: Vec::new(),
                string_map: Default::default(),
                bytes_map: Default::default(),
            },
        },
        PayloadCase {
            name: "large-bytes",
            message: PayloadShape {
                scalar: 1,
                sequence: 10,
                enabled: true,
                large_string: String::new(),
                large_bytes: vec![0xa5; 1024 * 1024],
                repeated_strings: Vec::new(),
                repeated_bytes: Vec::new(),
                nested: None,
                repeated_nested: Vec::new(),
                string_map: Default::default(),
                bytes_map: Default::default(),
            },
        },
        PayloadCase {
            name: "large-string",
            message: PayloadShape {
                scalar: 2,
                sequence: 20,
                enabled: true,
                large_string: "large string payload ".repeat(64 * 1024),
                large_bytes: Vec::new(),
                repeated_strings: Vec::new(),
                repeated_bytes: Vec::new(),
                nested: None,
                repeated_nested: Vec::new(),
                string_map: Default::default(),
                bytes_map: Default::default(),
            },
        },
        PayloadCase {
            name: "repeated-large-payloads",
            message: PayloadShape {
                scalar: 3,
                sequence: 30,
                enabled: true,
                large_string: String::new(),
                large_bytes: Vec::new(),
                repeated_strings: vec![
                    "repeated string payload a ".repeat(16 * 1024),
                    "repeated string payload b ".repeat(16 * 1024),
                ],
                repeated_bytes: vec![
                    vec![0x11; 256 * 1024],
                    vec![0x22; 256 * 1024],
                    vec![0x33; 256 * 1024],
                ],
                nested: None,
                repeated_nested: Vec::new(),
                string_map: Default::default(),
                bytes_map: Default::default(),
            },
        },
        PayloadCase {
            name: "mixed-scalar-payloads",
            message: mixed_payload_shape(),
        },
    ]
}

fn mixed_payload_shape() -> PayloadShape {
    let nested_a = NestedPayload {
        id: 7,
        label: "nested payload a ".repeat(8 * 1024),
        payload: vec![0x44; 128 * 1024],
    };
    let nested_b = NestedPayload {
        id: 8,
        label: "nested payload b ".repeat(8 * 1024),
        payload: vec![0x55; 128 * 1024],
    };

    PayloadShape {
        scalar: 42,
        sequence: 65_536,
        enabled: true,
        large_string: "mixed root string ".repeat(32 * 1024),
        large_bytes: vec![0x66; 512 * 1024],
        repeated_strings: vec![
            "mixed repeated string a ".repeat(8 * 1024),
            "mixed repeated string b ".repeat(8 * 1024),
        ],
        repeated_bytes: vec![vec![0x77; 192 * 1024], vec![0x88; 192 * 1024]],
        nested: Some(nested_a.clone()),
        repeated_nested: vec![nested_a, nested_b],
        string_map: [
            (
                "string-map-a".to_owned(),
                "mixed string map value a ".repeat(8 * 1024),
            ),
            (
                "string-map-b".to_owned(),
                "mixed string map value b ".repeat(8 * 1024),
            ),
        ]
        .into_iter()
        .collect(),
        bytes_map: [
            ("bytes-map-a".to_owned(), vec![0x99; 96 * 1024]),
            ("bytes-map-b".to_owned(), vec![0xaa; 96 * 1024]),
        ]
        .into_iter()
        .collect(),
    }
}

fn requests_per_iteration(encoded_len: usize) -> usize {
    BATCH_TARGET_BYTES.div_ceil(encoded_len)
}

#[cfg(feature = "hardware-dsa")]
fn concurrent_batches_per_iteration(encoded_len: usize, concurrent_requests: usize) -> usize {
    requests_per_iteration(encoded_len).div_ceil(concurrent_requests)
}

fn payload_corpus(message: &PayloadShape) -> Vec<PayloadShape> {
    let encoded_len = message.encoded_len();
    let corpus_len = (PAYLOAD_CORPUS_BYTES / encoded_len).clamp(8, 262_144);

    let mut corpus = Vec::with_capacity(corpus_len);
    for index in 0..corpus_len {
        let mut message = message.clone();
        perturb_payload_shape(&mut message, index as u8);
        corpus.push(message);
    }
    corpus
}

fn output_corpus(corpus_len: usize, encoded_len: usize) -> Vec<Vec<u8>> {
    (0..corpus_len)
        .map(|_| Vec::with_capacity(encoded_len))
        .collect()
}

fn next_payload_index(corpus: &[PayloadShape], request_index: &mut usize) -> usize {
    let index = *request_index % corpus.len();
    *request_index = request_index.wrapping_add(1);
    index
}
fn perturb_payload_shape(message: &mut PayloadShape, seed: u8) {
    perturb_string(&mut message.large_string, seed);
    perturb_bytes(&mut message.large_bytes, seed);
    for value in &mut message.repeated_strings {
        perturb_string(value, seed);
    }
    for value in &mut message.repeated_bytes {
        perturb_bytes(value, seed);
    }
    if let Some(nested) = &mut message.nested {
        perturb_nested_payload(nested, seed);
    }
    for nested in &mut message.repeated_nested {
        perturb_nested_payload(nested, seed);
    }
    for value in message.string_map.values_mut() {
        perturb_string(value, seed);
    }
    for value in message.bytes_map.values_mut() {
        perturb_bytes(value, seed);
    }
}

fn perturb_nested_payload(message: &mut NestedPayload, seed: u8) {
    perturb_string(&mut message.label, seed);
    perturb_bytes(&mut message.payload, seed);
}

fn perturb_string(value: &mut String, seed: u8) {
    if value.is_empty() {
        return;
    }
    let replacement = char::from(b'a' + seed % 26).to_string();
    value.replace_range(0..1, &replacement);
}

fn perturb_bytes(value: &mut [u8], seed: u8) {
    if let Some(first) = value.first_mut() {
        *first = first.wrapping_add(seed);
    }
    if let Some(last) = value.last_mut() {
        *last = last.wrapping_sub(seed);
    }
}

fn payload_count(message: &PayloadShape) -> usize {
    usize::from(!message.large_string.is_empty())
        + usize::from(!message.large_bytes.is_empty())
        + message.repeated_strings.len()
        + message.repeated_bytes.len()
        + message.nested.as_ref().map_or(0, nested_payload_count)
        + message
            .repeated_nested
            .iter()
            .map(nested_payload_count)
            .sum::<usize>()
        + message.string_map.len()
        + message.bytes_map.len()
}

fn nested_payload_count(message: &NestedPayload) -> usize {
    usize::from(!message.label.is_empty()) + usize::from(!message.payload.is_empty())
}

fn poll_until_ready<F>(future: F) -> F::Output
where
    F: Future,
{
    let mut future = std::pin::pin!(future);
    let waker = std::task::Waker::noop();
    let mut cx = Context::from_waker(waker);

    loop {
        match future.as_mut().poll(&mut cx) {
            Poll::Ready(output) => return output,
            Poll::Pending => core::hint::spin_loop(),
        }
    }
}

#[cfg(feature = "hardware-dsa")]
struct HardwareDsaWorkQueue {
    device_path: PathBuf,
    portal: WqPortal,
    dedicated: bool,
}

#[cfg(feature = "hardware-dsa")]
impl HardwareDsaWorkQueue {
    fn open_from_env() -> Option<Self> {
        let device_path = env::var_os("TONIC_DSA_WQ").map(PathBuf::from)?;
        assert!(
            device_path.exists(),
            "DSA work-queue device does not exist: {}",
            device_path.display()
        );

        let dedicated = detect_wq_mode(&device_path);
        let portal = WqPortal::open(&device_path)
            .unwrap_or_else(|err| panic!("failed to open {}: {err}", device_path.display()));

        Some(Self {
            device_path,
            portal,
            dedicated,
        })
    }

    fn submit(&self, desc: &DsaHwDesc) {
        // SAFETY: `DsaPayloadState` keeps the descriptor, completion record,
        // source message bytes, and destination buffer alive until hardware
        // completion reaches a terminal status.
        unsafe { self.portal.submit_dsa(desc, self.dedicated) };
    }
}

#[cfg(feature = "hardware-dsa")]
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

#[cfg(feature = "hardware-dsa")]
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

#[cfg(feature = "hardware-dsa")]
impl std::error::Error for HardwareDsaError {}

#[cfg(feature = "hardware-dsa")]
struct DsaPayloadTarget<'a> {
    work_queue: &'a HardwareDsaWorkQueue,
    buf: &'a mut Vec<u8>,
    submitted_payloads: usize,
    profile_active: bool,
    completed_payloads: usize,
}

#[cfg(feature = "hardware-dsa")]
impl<'a> DsaPayloadTarget<'a> {
    fn new(work_queue: &'a HardwareDsaWorkQueue, buf: &'a mut Vec<u8>) -> Self {
        Self {
            work_queue,
            buf,
            submitted_payloads: 0,
            completed_payloads: 0,
            profile_active: dsa_profile_active(),
        }
    }

    fn start_payload_copy(
        &mut self,
        payload: &[u8],
        state: &mut DsaPayloadState,
    ) -> Result<(), HardwareDsaError> {
        let profile_start = dsa_profile_start(self.profile_active);
        let len = payload.len();
        let xfer_size =
            u32::try_from(len).map_err(|_| HardwareDsaError::TransferTooLarge { len })?;
        let dst_offset = self.buf.len();
        self.buf.reserve(len);
        let dst = unsafe { self.buf.as_mut_ptr().add(dst_offset) };

        let touch_start = profile_start.as_ref().map(|_| Instant::now());
        touch_pages_for_dsa(payload.as_ptr(), dst, len);
        dsa_profile_record_touch_pages(touch_start);
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
        let submit_start = profile_start.as_ref().map(|_| Instant::now());
        self.work_queue.submit(&pending.desc);
        dsa_profile_record_portal_submit(submit_start);
        pending.submitted = true;
        self.submitted_payloads += 1;
        dsa_profile_record_payload_copy(profile_start, len);
        Ok(())
    }
}

#[cfg(feature = "hardware-dsa")]
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
                    pending.poll_completion(&self.work_queue.device_path, self.profile_active)
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

#[cfg(feature = "hardware-dsa")]
enum DsaPayloadState {
    Idle,
    Pending(PendingDsaPayloadCopy),
}

#[cfg(feature = "hardware-dsa")]
impl Default for DsaPayloadState {
    fn default() -> Self {
        Self::Idle
    }
}

#[cfg(feature = "hardware-dsa")]
struct PendingDsaPayloadCopy {
    desc: DsaHwDesc,
    completion: DsaCompletionRecord,
    dst_offset: usize,
    len: usize,
    submitted: bool,
    _pin: PhantomPinned,
}

#[cfg(feature = "hardware-dsa")]
impl PendingDsaPayloadCopy {
    fn poll_completion(
        &mut self,
        device_path: &PathBuf,
        profile_active: bool,
    ) -> Poll<Result<(usize, usize), HardwareDsaError>> {
        let profile_start = dsa_profile_start(profile_active);
        let raw_status = self.completion.status();
        let status = DsaCompletionStatus::mask(raw_status);
        let pending = status == DsaCompletionStatus::None.as_u8();
        dsa_profile_record_completion_poll(profile_start, pending);
        if pending {
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

#[cfg(feature = "hardware-dsa")]
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

#[cfg(feature = "hardware-dsa")]
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

fn criterion_config() -> Criterion {
    Criterion::default()
        .warm_up_time(Duration::from_millis(1))
        .measurement_time(Duration::from_secs(1))
        .sample_size(10)
        .with_profiler(DsaSubmissionProfiler)
}

criterion_group! {
    name = benches;
    config = criterion_config();
    targets = benchmark_async_payload_encode
}
criterion_main!(benches);
