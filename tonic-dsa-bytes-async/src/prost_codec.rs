use bytes::BufMut;
use prost::{
    Message,
    encoding::{encode_varint, encoded_len_varint},
    transfer::{AsyncEncodeTarget, EncodeOptions, EncodePayload, PollEncodeState},
};
use std::{
    fmt,
    future::{Future, Ready, ready},
    marker::{PhantomData, PhantomPinned},
    pin::Pin,
    task::{Context, Poll},
};
use tonic::Status;
use tonic::codec::{BufferSettings, Codec, DecodeBuf, Decoder, EncodeBuf, EncodeBuffer, Encoder};

use crate::{
    DsaCompletionRecord, DsaCompletionStatus, DsaHwDesc, DsaMemmoveRetry, DsaRetryAction,
    SharedDsaWorkQueue, process_dsa_work_queue, submit_retry, touch_pages_for_dsa,
};

/// A tonic Prost codec that can asynchronously copy large string/bytes fields with DSA.
#[derive(Debug, Clone)]
pub struct DsaAsyncProstCodec<T, U> {
    work_queue: Option<SharedDsaWorkQueue>,
    _pd: PhantomData<(T, U)>,
}

impl<T, U> DsaAsyncProstCodec<T, U> {
    /// Creates a Prost codec using the process DSA work queue when configured.
    pub fn new() -> Self {
        Self::with_optional_work_queue(process_dsa_work_queue())
    }

    /// Creates a Prost codec with an explicit shared DSA work queue.
    pub fn with_work_queue(work_queue: SharedDsaWorkQueue) -> Self {
        Self::with_optional_work_queue(Some(work_queue))
    }

    /// Creates a Prost codec that keeps the async DSA type but encodes on the CPU path.
    pub fn without_work_queue() -> Self {
        Self::with_optional_work_queue(None)
    }

    fn with_optional_work_queue(work_queue: Option<SharedDsaWorkQueue>) -> Self {
        Self {
            work_queue,
            _pd: PhantomData,
        }
    }

    /// Builds a Prost encoder with explicit tonic buffer settings.
    pub fn raw_encoder(buffer_settings: BufferSettings) -> DsaAsyncProstEncoder<T> {
        DsaAsyncProstEncoder::new(buffer_settings)
    }

    /// Builds a Prost encoder with an explicit shared work queue.
    pub fn raw_encoder_with_work_queue(
        buffer_settings: BufferSettings,
        work_queue: SharedDsaWorkQueue,
    ) -> DsaAsyncProstEncoder<T> {
        DsaAsyncProstEncoder::with_work_queue(buffer_settings, work_queue)
    }

    /// Builds a Prost decoder with explicit tonic buffer settings.
    pub fn raw_decoder(buffer_settings: BufferSettings) -> DsaAsyncProstDecoder<U> {
        DsaAsyncProstDecoder::new(buffer_settings)
    }
}

impl<T, U> Default for DsaAsyncProstCodec<T, U> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T, U> Codec for DsaAsyncProstCodec<T, U>
where
    T: Message + Send + 'static,
    U: Message + Default + Send + 'static,
{
    type Encode = T;
    type Decode = U;

    type Encoder = DsaAsyncProstEncoder<T>;
    type Decoder = DsaAsyncProstDecoder<U>;

    fn encoder(&mut self) -> Self::Encoder {
        DsaAsyncProstEncoder::with_optional_work_queue(
            BufferSettings::default(),
            self.work_queue.clone(),
        )
    }

    fn decoder(&mut self) -> Self::Decoder {
        DsaAsyncProstDecoder::new(BufferSettings::default())
    }
}

/// A Prost encoder that can asynchronously copy selected string/bytes payloads with DSA.
#[derive(Clone)]
pub struct DsaAsyncProstEncoder<T> {
    buffer_settings: BufferSettings,
    work_queue: Option<SharedDsaWorkQueue>,
    #[cfg(test)]
    yielding_cpu_for_tests: bool,
    _pd: PhantomData<T>,
}

impl<T> fmt::Debug for DsaAsyncProstEncoder<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DsaAsyncProstEncoder")
            .field("buffer_settings", &self.buffer_settings)
            .field("work_queue", &self.work_queue)
            .finish()
    }
}

impl<T> Default for DsaAsyncProstEncoder<T> {
    fn default() -> Self {
        Self::new(BufferSettings::default())
    }
}

impl<T> DsaAsyncProstEncoder<T> {
    /// Gets a new Prost encoder using the process DSA work queue when configured.
    pub fn new(buffer_settings: BufferSettings) -> Self {
        Self::with_optional_work_queue(buffer_settings, process_dsa_work_queue())
    }

    /// Gets a new Prost encoder with an explicit shared DSA work queue.
    pub fn with_work_queue(
        buffer_settings: BufferSettings,
        work_queue: SharedDsaWorkQueue,
    ) -> Self {
        Self::with_optional_work_queue(buffer_settings, Some(work_queue))
    }

    /// Gets a Prost encoder that keeps the async DSA type but encodes on the CPU path.
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
            #[cfg(test)]
            yielding_cpu_for_tests: false,
            _pd: PhantomData,
        }
    }
}

impl<T> Encoder for DsaAsyncProstEncoder<T>
where
    T: Message + Send + 'static,
{
    type Item = T;
    type Error = Status;
    type Encode = DsaAsyncProstEncode<T>;

    fn encode(
        self: Pin<&mut Self>,
        item: Self::Item,
        dst: EncodeBuffer,
    ) -> Result<Self::Encode, Self::Error> {
        let work_queue = self.as_ref().get_ref().work_queue.clone();
        #[cfg(test)]
        let yielding_cpu_for_tests = self.as_ref().get_ref().yielding_cpu_for_tests;

        #[cfg(not(test))]
        let sink = DsaProstEncodeSink::new(dst, work_queue);
        #[cfg(test)]
        let sink = DsaProstEncodeSink::new_for_tests(dst, work_queue, yielding_cpu_for_tests);

        Ok(DsaAsyncProstEncode::new(
            item,
            sink,
            EncodeOptions::default(),
        ))
    }

    fn buffer_settings(&self) -> BufferSettings {
        self.buffer_settings
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum DsaAsyncProstEncodeStage {
    Start,
    Body,
    Done,
}

/// Owned future returned by [`DsaAsyncProstEncoder`] for one message encode.
pub struct DsaAsyncProstEncode<T> {
    // Drop order is part of the safety contract: pending payload state may hold
    // a DSA descriptor that references bytes inside `item` and uninitialized
    // destination storage inside `sink`. Rust drops fields in declaration order,
    // so the state must drain any in-flight descriptor before either owner goes
    // away.
    state: PollEncodeState<DsaProstEncodeSink>,
    item: Option<T>,
    sink: Option<DsaProstEncodeSink>,
    options: EncodeOptions,
    stage: DsaAsyncProstEncodeStage,
}

impl<T> DsaAsyncProstEncode<T> {
    fn new(item: T, sink: DsaProstEncodeSink, options: EncodeOptions) -> Self {
        Self {
            item: Some(item),
            sink: Some(sink),
            state: PollEncodeState::default(),
            options,
            stage: DsaAsyncProstEncodeStage::Start,
        }
    }
}

impl<T> fmt::Debug for DsaAsyncProstEncode<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DsaAsyncProstEncode")
            .field("stage", &self.stage)
            .finish_non_exhaustive()
    }
}

impl<T> Future for DsaAsyncProstEncode<T>
where
    T: Message,
{
    type Output = Result<EncodeBuffer, Status>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // SAFETY: once tonic polls this future, the poll encode state remains
        // in place until the future completes or is dropped.
        let this = unsafe { self.get_unchecked_mut() };

        loop {
            match this.stage {
                DsaAsyncProstEncodeStage::Start => {
                    let item = this.item.as_ref().expect("encode item present");
                    let sink = this.sink.as_mut().expect("encode sink present");
                    let len = item.encoded_len();
                    let required = if this.options.length_delimited {
                        len + encoded_len_varint(len as u64)
                    } else {
                        len
                    };
                    let remaining = {
                        let buf = sink.buf_mut();
                        buf.remaining_mut()
                    };
                    if required > remaining {
                        this.stage = DsaAsyncProstEncodeStage::Done;
                        return Poll::Ready(Err(Status::internal(format!(
                            "failed to encode Protobuf message; insufficient buffer capacity (required: {required}, remaining: {remaining})"
                        ))));
                    }

                    if this.options.length_delimited {
                        let mut buf = sink.buf_mut();
                        encode_varint(len as u64, &mut buf);
                    }
                    this.stage = DsaAsyncProstEncodeStage::Body;
                }
                DsaAsyncProstEncodeStage::Body => {
                    let item = this.item.as_ref().expect("encode item present");
                    let sink = this.sink.as_mut().expect("encode sink present");
                    match item.poll_encode_raw(sink, &mut this.state, cx) {
                        Poll::Ready(Ok(())) => {
                            this.stage = DsaAsyncProstEncodeStage::Done;
                            this.item.take();
                            let sink = this.sink.take().expect("encode sink present");
                            return Poll::Ready(Ok(sink.into_inner()));
                        }
                        Poll::Ready(Err(status)) => {
                            this.stage = DsaAsyncProstEncodeStage::Done;
                            return Poll::Ready(Err(status));
                        }
                        Poll::Pending => return Poll::Pending,
                    }
                }
                DsaAsyncProstEncodeStage::Done => {
                    panic!("DsaAsyncProstEncode polled after completion")
                }
            }
        }
    }
}

/// A Prost decoder paired with [`DsaAsyncProstEncoder`].
#[derive(Debug, Clone)]
pub struct DsaAsyncProstDecoder<U> {
    buffer_settings: BufferSettings,
    _pd: PhantomData<U>,
}

impl<U> DsaAsyncProstDecoder<U> {
    /// Gets a new Prost decoder with explicit tonic buffer settings.
    pub fn new(buffer_settings: BufferSettings) -> Self {
        Self {
            buffer_settings,
            _pd: PhantomData,
        }
    }
}

impl<U> Default for DsaAsyncProstDecoder<U> {
    fn default() -> Self {
        Self::new(BufferSettings::default())
    }
}

impl<U> Decoder for DsaAsyncProstDecoder<U>
where
    U: Message + Default + Send + 'static,
{
    type Item = U;
    type Error = Status;
    type Decode = Ready<Result<Option<U>, Status>>;

    fn decode(self: Pin<&mut Self>, buf: DecodeBuf<'_>) -> Result<Self::Decode, Self::Error> {
        let _ = self;
        Ok(ready(Ok(Some(
            Message::decode(buf).map_err(from_decode_error)?,
        ))))
    }

    fn buffer_settings(&self) -> BufferSettings {
        self.buffer_settings
    }
}

fn from_decode_error(error: prost::DecodeError) -> Status {
    Status::internal(error.to_string())
}

#[derive(Debug)]
struct DsaProstEncodeSink {
    buf: EncodeBuffer,
    work_queue: Option<SharedDsaWorkQueue>,
    #[cfg(test)]
    yielding_cpu_for_tests: bool,
}

impl DsaProstEncodeSink {
    #[cfg(not(test))]
    fn new(buf: EncodeBuffer, work_queue: Option<SharedDsaWorkQueue>) -> Self {
        Self {
            buf,
            work_queue,
            #[cfg(test)]
            yielding_cpu_for_tests: false,
        }
    }

    #[cfg(test)]
    fn new_for_tests(
        buf: EncodeBuffer,
        work_queue: Option<SharedDsaWorkQueue>,
        yielding_cpu_for_tests: bool,
    ) -> Self {
        Self {
            buf,
            work_queue,
            yielding_cpu_for_tests,
        }
    }

    fn into_inner(self) -> EncodeBuffer {
        self.buf
    }

    fn selected_work_queue(&self, payload_len: usize) -> Option<SharedDsaWorkQueue> {
        self.work_queue
            .as_ref()
            .filter(|work_queue| work_queue.accelerates(payload_len))
            .cloned()
    }
}

impl AsyncEncodeTarget for DsaProstEncodeSink {
    type Error = Status;

    type BufMut<'a>
        = EncodeBuf<'a>
    where
        Self: 'a;

    type PayloadState = DsaProstPayloadState;

    fn encode_error(error: prost::EncodeError) -> Self::Error {
        Status::internal(error.to_string())
    }

    fn buf_mut(&mut self) -> Self::BufMut<'_> {
        self.buf.as_encode_buf()
    }

    fn poll_write_payload(
        &mut self,
        payload: EncodePayload<'_>,
        state: Pin<&mut Self::PayloadState>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        let payload = payload.as_bytes();
        // SAFETY: the caller pins the payload state for the duration of any
        // pending hardware copy. We only replace the enum while it is Ready.
        let state = unsafe { state.get_unchecked_mut() };

        if matches!(state, DsaProstPayloadState::Ready) {
            #[cfg(test)]
            if self.yielding_cpu_for_tests {
                *state = DsaProstPayloadState::YieldingCpu(PendingYieldingProstPayloadCopy::new());
            } else {
                self.start_payload_copy(payload, state)?;
            }

            #[cfg(not(test))]
            self.start_payload_copy(payload, state)?;
        }

        match state {
            DsaProstPayloadState::Ready => Poll::Ready(Ok(())),
            DsaProstPayloadState::Dsa(copy) => {
                // SAFETY: the DSA payload state is pinned by the caller while
                // the descriptor may reference its completion record.
                match unsafe { Pin::new_unchecked(copy) }.poll(payload, &mut self.buf, cx) {
                    Poll::Ready(Ok(())) => {
                        *state = DsaProstPayloadState::Ready;
                        Poll::Ready(Ok(()))
                    }
                    Poll::Ready(Err(status)) => Poll::Ready(Err(status)),
                    Poll::Pending => Poll::Pending,
                }
            }
            #[cfg(test)]
            DsaProstPayloadState::YieldingCpu(copy) => {
                match copy.poll(payload, &mut self.buf, cx) {
                    Poll::Ready(Ok(())) => {
                        *state = DsaProstPayloadState::Ready;
                        Poll::Ready(Ok(()))
                    }
                    Poll::Ready(Err(status)) => Poll::Ready(Err(status)),
                    Poll::Pending => Poll::Pending,
                }
            }
        }
    }
}

impl DsaProstEncodeSink {
    fn start_payload_copy(
        &mut self,
        payload: &[u8],
        state: &mut DsaProstPayloadState,
    ) -> Result<(), Status> {
        match self.selected_work_queue(payload.len()) {
            None => {
                self.buf.as_encode_buf().put_slice(payload);
                Ok(())
            }
            Some(_) if payload.len() > u32::MAX as usize => Err(Status::internal(format!(
                "async dsa prost encode cannot copy {} bytes; maximum DSA transfer is {} bytes",
                payload.len(),
                u32::MAX
            ))),
            Some(work_queue) => {
                *state = DsaProstPayloadState::Dsa(PendingDsaProstPayloadCopy::new(work_queue));
                Ok(())
            }
        }
    }
}

enum DsaProstPayloadState {
    Ready,
    Dsa(PendingDsaProstPayloadCopy),
    #[cfg(test)]
    YieldingCpu(PendingYieldingProstPayloadCopy),
}

impl Default for DsaProstPayloadState {
    fn default() -> Self {
        Self::Ready
    }
}

impl fmt::Debug for DsaProstPayloadState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DsaProstPayloadState::Ready => f.write_str("DsaProstPayloadState::Ready"),
            DsaProstPayloadState::Dsa(_) => f.write_str("DsaProstPayloadState::Dsa"),
            #[cfg(test)]
            DsaProstPayloadState::YieldingCpu(_) => {
                f.write_str("DsaProstPayloadState::YieldingCpu")
            }
        }
    }
}

#[cfg(test)]
struct PendingYieldingProstPayloadCopy {
    yielded: bool,
}

#[cfg(test)]
impl PendingYieldingProstPayloadCopy {
    fn new() -> Self {
        Self { yielded: false }
    }

    fn poll(
        &mut self,
        source: &[u8],
        buf: &mut EncodeBuffer,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), Status>> {
        if !self.yielded {
            self.yielded = true;
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }

        buf.as_encode_buf().put_slice(source);
        Poll::Ready(Ok(()))
    }
}

struct PendingDsaProstPayloadCopy {
    work_queue: SharedDsaWorkQueue,
    source: *const u8,
    source_len: usize,
    dst: *mut u8,
    dst_len: usize,
    desc: DsaHwDesc,
    completion: DsaCompletionRecord,
    retry: Option<DsaMemmoveRetry>,
    submitted: bool,
    completed: bool,
    _pin: PhantomPinned,
}

impl fmt::Debug for PendingDsaProstPayloadCopy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("PendingDsaProstPayloadCopy")
    }
}

// SAFETY: `source` points into the encoded message owned by the top-level
// encode future, and `dst` points into the `EncodeBuffer` owned by the sink.
// The descriptor and completion record are pinned while submitted. `Drop`
// drains a submitted descriptor before the state can be released.
unsafe impl Send for PendingDsaProstPayloadCopy {}

impl PendingDsaProstPayloadCopy {
    fn new(work_queue: SharedDsaWorkQueue) -> Self {
        Self {
            work_queue,
            source: core::ptr::null(),
            source_len: 0,
            dst: std::ptr::null_mut(),
            dst_len: 0,
            desc: DsaHwDesc::default(),
            completion: DsaCompletionRecord::default(),
            retry: None,
            submitted: false,
            completed: false,
            _pin: PhantomPinned,
        }
    }

    fn poll(
        mut self: Pin<&mut Self>,
        source: &[u8],
        buf: &mut EncodeBuffer,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), Status>> {
        // SAFETY: This method never moves fields out of the pinned state. It
        // only mutates scalar state and borrows the owned descriptor,
        // completion record, and encode buffer in place.
        let this = unsafe { self.as_mut().get_unchecked_mut() };

        if !this.submitted {
            let len = source.len();
            let mut dst = buf.as_encode_buf();
            // SAFETY: The pending state has exclusive access to the encode
            // buffer through the poll call until completion. The matching
            // advance happens exactly once after DSA reports success.
            let dst_ptr = unsafe { dst.reserve_uninit_slice_for_pending(len) };
            touch_pages_for_dsa(source.as_ptr(), dst_ptr, len);

            let retry = DsaMemmoveRetry::new(source.as_ptr(), dst_ptr, len);
            submit_retry(
                &this.work_queue,
                &mut this.desc,
                &mut this.completion,
                &retry,
            )?;
            this.retry = Some(retry);
            this.source = source.as_ptr();
            this.source_len = len;
            this.dst = dst_ptr;
            this.dst_len = len;
            this.submitted = true;
        } else {
            debug_assert_eq!(this.source, source.as_ptr());
            debug_assert_eq!(this.source_len, source.len());
        }

        if this.completion.status() == DsaCompletionStatus::None.as_u8() {
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }

        let retry = this.retry.as_mut().expect("pending retry state available");
        match retry.handle_completion(this.completion, &this.work_queue.config.device_path) {
            Ok(DsaRetryAction::Complete) => {
                this.completed = true;
                // SAFETY: DSA success means the descriptor initialized exactly
                // the bytes reserved for this payload copy.
                unsafe {
                    buf.as_encode_buf()
                        .advance_reserved_uninit_slice(this.dst_len);
                }
                Poll::Ready(Ok(()))
            }
            Ok(DsaRetryAction::Retry) => {
                submit_retry(
                    &this.work_queue,
                    &mut this.desc,
                    &mut this.completion,
                    retry,
                )?;
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            Err(status) => {
                this.completed = true;
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
}

impl Drop for PendingDsaProstPayloadCopy {
    fn drop(&mut self) {
        self.drain_completion();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use http_body::Body;
    use std::pin::pin;
    use tonic::codec::{EncodeBody, HEADER_SIZE};

    #[derive(Clone, PartialEq, prost::Message)]
    struct LargeFields {
        #[prost(string, tag = "1")]
        name: String,
        #[prost(bytes = "bytes", tag = "2")]
        payload: Bytes,
        #[prost(uint32, tag = "3")]
        id: u32,
    }

    impl<T> DsaAsyncProstEncoder<T> {
        fn yielding_cpu_for_tests(buffer_settings: BufferSettings) -> Self {
            Self {
                buffer_settings,
                work_queue: None,
                yielding_cpu_for_tests: true,
                _pd: PhantomData,
            }
        }
    }

    #[test]
    fn prost_encoder_without_work_queue_uses_cpu_path() {
        let msg = LargeFields {
            name: "cpu prost string".repeat(16),
            payload: Bytes::from(vec![0x5a; 1024]),
            id: 7,
        };
        let data = encode_one_frame(
            DsaAsyncProstEncoder::<LargeFields>::without_work_queue(BufferSettings::default()),
            msg.clone(),
        );
        assert_decodes_frame(data, msg);
    }

    #[test]
    fn prost_encoder_can_pending_on_payload_copy() {
        let msg = LargeFields {
            name: "pending prost string".repeat(16),
            payload: Bytes::from(vec![0xa5; 1024]),
            id: 9,
        };
        let source = tokio_stream::iter(std::iter::once(Ok::<_, Status>(msg.clone())));
        let mut body = pin!(EncodeBody::new_client(
            DsaAsyncProstEncoder::<LargeFields>::yielding_cpu_for_tests(BufferSettings::default()),
            source,
            None,
            None,
        ));
        let waker = std::task::Waker::noop();
        let mut cx = Context::from_waker(waker);
        let mut saw_pending = false;

        let frame = loop {
            match body.as_mut().poll_frame(&mut cx) {
                Poll::Ready(Some(Ok(frame))) => break frame,
                Poll::Ready(Some(Err(status))) => panic!("encode failed: {status}"),
                Poll::Ready(None) => panic!("body ended before data"),
                Poll::Pending => saw_pending = true,
            }
        };

        assert!(saw_pending, "payload copy did not return Pending");
        let data = frame.into_data().expect("got data frame");
        assert_decodes_frame(data, msg);
    }

    fn encode_one_frame(encoder: DsaAsyncProstEncoder<LargeFields>, msg: LargeFields) -> Bytes {
        let source = tokio_stream::iter(std::iter::once(Ok::<_, Status>(msg)));
        let mut body = pin!(EncodeBody::new_client(encoder, source, None, None));
        let waker = std::task::Waker::noop();
        let mut cx = Context::from_waker(waker);

        match body.as_mut().poll_frame(&mut cx) {
            Poll::Ready(Some(Ok(frame))) => frame.into_data().expect("got data frame"),
            Poll::Ready(Some(Err(status))) => panic!("encode failed: {status}"),
            Poll::Ready(None) => panic!("body ended before data"),
            Poll::Pending => panic!("CPU encode should not be pending"),
        }
    }

    fn assert_decodes_frame(data: Bytes, expected: LargeFields) {
        assert_eq!(data[0], 0);
        assert_eq!(
            u32::from_be_bytes(data[1..HEADER_SIZE].try_into().unwrap()) as usize,
            data.len() - HEADER_SIZE
        );

        let decoded = LargeFields::decode(&data[HEADER_SIZE..]).expect("prost decode succeeds");
        assert_eq!(decoded, expected);
    }
}
