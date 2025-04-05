// Copyright 2025 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::cmp::Ordering;
use std::collections::VecDeque;
use std::fmt::Debug;
use std::future::{Future, poll_fn};
use std::pin::pin;
use std::sync::Arc;
use std::task::Poll;
use std::time::Instant;

use await_tree::InstrumentAwait;
use either::Either;
use futures::future::BoxFuture;
use futures::{TryFuture, TryFutureExt};
use risingwave_common::array::StreamChunk;
use risingwave_common::bail;
use risingwave_common::bitmap::Bitmap;
use risingwave_common::metrics::{LabelGuardedIntCounter, LabelGuardedIntGauge};
use risingwave_common::util::epoch::{EpochPair, INVALID_EPOCH};
use risingwave_common_estimate_size::EstimateSize;
use risingwave_common_rate_limit::{RateLimit, RateLimiter};
use tokio::select;
use tokio::sync::mpsc::UnboundedReceiver;

pub type LogStoreResult<T> = Result<T, anyhow::Error>;
pub type ChunkId = usize;

#[derive(Debug, PartialEq, Copy, Clone)]
pub enum TruncateOffset {
    Chunk { epoch: u64, chunk_id: ChunkId },
    Barrier { epoch: u64 },
}

impl PartialOrd for TruncateOffset {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let extract = |offset: &TruncateOffset| match offset {
            TruncateOffset::Chunk { epoch, chunk_id } => (*epoch, *chunk_id),
            TruncateOffset::Barrier { epoch } => (*epoch, usize::MAX),
        };
        let this = extract(self);
        let other = extract(other);
        this.partial_cmp(&other)
    }
}

impl TruncateOffset {
    pub fn next_chunk_id(&self) -> ChunkId {
        match self {
            TruncateOffset::Chunk { chunk_id, .. } => chunk_id + 1,
            TruncateOffset::Barrier { .. } => 0,
        }
    }

    pub fn epoch(&self) -> u64 {
        match self {
            TruncateOffset::Chunk { epoch, .. } | TruncateOffset::Barrier { epoch } => *epoch,
        }
    }

    pub fn check_next_offset(&self, next_offset: TruncateOffset) -> LogStoreResult<()> {
        if *self >= next_offset {
            bail!(
                "next offset {:?} should be later than current offset {:?}",
                next_offset,
                self
            )
        } else {
            Ok(())
        }
    }

    pub fn check_next_item_epoch(&self, epoch: u64) -> LogStoreResult<()> {
        match self {
            TruncateOffset::Chunk {
                epoch: offset_epoch,
                ..
            } => {
                if epoch != *offset_epoch {
                    bail!(
                        "new item epoch {} does not match current chunk offset epoch {}",
                        epoch,
                        offset_epoch
                    );
                }
            }
            TruncateOffset::Barrier {
                epoch: offset_epoch,
            } => {
                if epoch <= *offset_epoch {
                    bail!(
                        "new item epoch {} does not exceed barrier offset epoch {}",
                        epoch,
                        offset_epoch
                    );
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug)]
pub enum LogStoreReadItem {
    StreamChunk {
        chunk: StreamChunk,
        chunk_id: ChunkId,
    },
    Barrier {
        is_checkpoint: bool,
        new_vnode_bitmap: Option<Arc<Bitmap>>,
        is_stop: bool,
    },
}

pub trait LogWriterPostFlushCurrentEpochFn<'a> = FnOnce() -> BoxFuture<'a, LogStoreResult<()>>;

#[must_use]
pub struct LogWriterPostFlushCurrentEpoch<'a>(
    Box<dyn LogWriterPostFlushCurrentEpochFn<'a> + Send + 'a>,
);

impl<'a> LogWriterPostFlushCurrentEpoch<'a> {
    pub fn new(f: impl LogWriterPostFlushCurrentEpochFn<'a> + Send + 'a) -> Self {
        Self(Box::new(f))
    }

    pub async fn post_yield_barrier(self) -> LogStoreResult<()> {
        self.0().await
    }
}

pub struct FlushCurrentEpochOptions {
    pub is_checkpoint: bool,
    pub new_vnode_bitmap: Option<Arc<Bitmap>>,
    pub is_stop: bool,
}

pub trait LogWriter: Send {
    /// Initialize the log writer with an epoch
    fn init(
        &mut self,
        epoch: EpochPair,
        pause_read_on_bootstrap: bool,
    ) -> impl Future<Output = LogStoreResult<()>> + Send + '_;

    /// Write a stream chunk to the log writer
    fn write_chunk(
        &mut self,
        chunk: StreamChunk,
    ) -> impl Future<Output = LogStoreResult<()>> + Send + '_;

    /// Mark current epoch as finished and sealed, and flush the unconsumed log data.
    fn flush_current_epoch(
        &mut self,
        next_epoch: u64,
        options: FlushCurrentEpochOptions,
    ) -> impl Future<Output = LogStoreResult<LogWriterPostFlushCurrentEpoch<'_>>> + Send + '_;

    fn pause(&mut self) -> LogStoreResult<()>;

    fn resume(&mut self) -> LogStoreResult<()>;
}

pub trait LogReader: Send + Sized + 'static {
    /// Initialize the log reader. Usually function as waiting for log writer to be initialized.
    fn init(&mut self) -> impl Future<Output = LogStoreResult<()>> + Send + '_;

    /// Consume log store from given `start_offset` or aligned start offset recorded previously.
    fn start_from(
        &mut self,
        start_offset: Option<u64>,
    ) -> impl Future<Output = LogStoreResult<()>> + Send + '_;

    /// Emit the next item.
    ///
    /// The implementation should ensure that the future is cancellation safe.
    fn next_item(
        &mut self,
    ) -> impl Future<Output = LogStoreResult<(u64, LogStoreReadItem)>> + Send + '_;

    /// Mark that all items emitted so far have been consumed and it is safe to truncate the log
    /// from the current offset.
    fn truncate(&mut self, offset: TruncateOffset) -> LogStoreResult<()>;

    /// Reset the log reader to after the latest truncate offset
    ///
    /// The return flag means whether the log store support rewind
    fn rewind(&mut self) -> impl Future<Output = LogStoreResult<()>> + Send + '_;
}

pub trait LogStoreFactory: Send + 'static {
    const ALLOW_REWIND: bool;
    const REBUILD_SINK_ON_UPDATE_VNODE_BITMAP: bool;
    type Reader: LogReader;
    type Writer: LogWriter;

    fn build(self) -> impl Future<Output = (Self::Reader, Self::Writer)> + Send;
}

pub struct TransformChunkLogReader<F: Fn(StreamChunk) -> StreamChunk, R: LogReader> {
    f: F,
    inner: R,
}

impl<F: Fn(StreamChunk) -> StreamChunk + Send + 'static, R: LogReader> LogReader
    for TransformChunkLogReader<F, R>
{
    fn init(&mut self) -> impl Future<Output = LogStoreResult<()>> + Send + '_ {
        self.inner.init()
    }

    async fn next_item(&mut self) -> LogStoreResult<(u64, LogStoreReadItem)> {
        let (epoch, item) = self.inner.next_item().await?;
        let item = match item {
            LogStoreReadItem::StreamChunk { chunk, chunk_id } => LogStoreReadItem::StreamChunk {
                chunk: (self.f)(chunk),
                chunk_id,
            },
            other => other,
        };
        Ok((epoch, item))
    }

    fn truncate(&mut self, offset: TruncateOffset) -> LogStoreResult<()> {
        self.inner.truncate(offset)
    }

    fn rewind(&mut self) -> impl Future<Output = LogStoreResult<()>> + Send + '_ {
        self.inner.rewind()
    }

    fn start_from(
        &mut self,
        start_offset: Option<u64>,
    ) -> impl Future<Output = LogStoreResult<()>> + Send + '_ {
        self.inner.start_from(start_offset)
    }
}

pub struct BackpressureMonitoredLogReader<R: LogReader> {
    inner: R,
    /// Start time to wait for new future after poll ready
    wait_new_future_start_time: Option<Instant>,
    wait_new_future_duration_ns: LabelGuardedIntCounter<4>,
}

impl<R: LogReader> BackpressureMonitoredLogReader<R> {
    fn new(inner: R, wait_new_future_duration_ns: LabelGuardedIntCounter<4>) -> Self {
        Self {
            inner,
            wait_new_future_start_time: None,
            wait_new_future_duration_ns,
        }
    }
}

impl<R: LogReader> LogReader for BackpressureMonitoredLogReader<R> {
    fn init(&mut self) -> impl Future<Output = LogStoreResult<()>> + Send + '_ {
        self.wait_new_future_start_time = None;
        self.inner.init()
    }

    fn next_item(
        &mut self,
    ) -> impl Future<Output = LogStoreResult<(u64, LogStoreReadItem)>> + Send + '_ {
        if let Some(start_time) = self.wait_new_future_start_time.take() {
            self.wait_new_future_duration_ns
                .inc_by(start_time.elapsed().as_nanos() as _);
        }
        self.inner.next_item().inspect_ok(|_| {
            // Set start time when return ready
            self.wait_new_future_start_time = Some(Instant::now());
        })
    }

    fn truncate(&mut self, offset: TruncateOffset) -> LogStoreResult<()> {
        self.inner.truncate(offset)
    }

    fn rewind(&mut self) -> impl Future<Output = LogStoreResult<()>> + Send + '_ {
        self.inner.rewind().inspect_ok(|_| {
            self.wait_new_future_start_time = None;
        })
    }

    fn start_from(
        &mut self,
        start_offset: Option<u64>,
    ) -> impl Future<Output = LogStoreResult<()>> + Send + '_ {
        self.inner.start_from(start_offset)
    }
}

pub struct MonitoredLogReader<R: LogReader> {
    inner: R,
    read_epoch: u64,
    metrics: LogReaderMetrics,
}

pub struct LogReaderMetrics {
    pub log_store_latest_read_epoch: LabelGuardedIntGauge<4>,
    pub log_store_read_rows: LabelGuardedIntCounter<4>,
    pub log_store_read_bytes: LabelGuardedIntCounter<4>,
    pub log_store_reader_wait_new_future_duration_ns: LabelGuardedIntCounter<4>,
}

impl<R: LogReader> MonitoredLogReader<R> {
    pub fn new(inner: R, metrics: LogReaderMetrics) -> Self {
        Self {
            inner,
            read_epoch: INVALID_EPOCH,
            metrics,
        }
    }
}

impl<R: LogReader> LogReader for MonitoredLogReader<R> {
    async fn init(&mut self) -> LogStoreResult<()> {
        self.inner.init().instrument_await("log_reader_init").await
    }

    async fn next_item(&mut self) -> LogStoreResult<(u64, LogStoreReadItem)> {
        self.inner
            .next_item()
            .instrument_await("log_reader_next_item")
            .await
            .inspect(|(epoch, item)| {
                if self.read_epoch != *epoch {
                    self.read_epoch = *epoch;
                    self.metrics.log_store_latest_read_epoch.set(*epoch as _);
                }
                if let LogStoreReadItem::StreamChunk { chunk, .. } = item {
                    self.metrics
                        .log_store_read_rows
                        .inc_by(chunk.cardinality() as _);
                    self.metrics
                        .log_store_read_bytes
                        .inc_by(chunk.estimated_size() as u64);
                }
            })
    }

    fn truncate(&mut self, offset: TruncateOffset) -> LogStoreResult<()> {
        self.inner.truncate(offset)
    }

    fn rewind(&mut self) -> impl Future<Output = LogStoreResult<()>> + Send + '_ {
        self.inner.rewind().instrument_await("log_reader_rewind")
    }

    fn start_from(
        &mut self,
        start_offset: Option<u64>,
    ) -> impl Future<Output = LogStoreResult<()>> + Send + '_ {
        self.inner.start_from(start_offset)
    }
}

type UpstreamChunkOffset = TruncateOffset;
type DownstreamChunkOffset = TruncateOffset;

struct SplitChunk {
    chunk: StreamChunk,
    upstream_chunk_offset: UpstreamChunkOffset,
    // Indicate whether this is the last split chunk from the same `upstream_chunk_offset`
    is_last: bool,
}

struct RateLimitedLogReaderCore<R: LogReader> {
    inner: R,
    // Newer items at the front
    consumed_offset_queue: VecDeque<(DownstreamChunkOffset, UpstreamChunkOffset)>,
    // Newer items at the front
    unconsumed_chunk_queue: VecDeque<SplitChunk>,
    next_chunk_id: usize,
}

impl<R: LogReader> RateLimitedLogReaderCore<R> {
    // Returns: Left - chunk, Right - Barrier/UpdateVnodeBitmap
    async fn next_item(&mut self) -> LogStoreResult<Either<SplitChunk, (u64, LogStoreReadItem)>> {
        // Get upstream chunk from unconsumed_chunk_queue first.
        // If unconsumed_chunk_queue is empty, get the chunk from the inner log reader.
        match self.unconsumed_chunk_queue.pop_back() {
            Some(split_chunk) => Ok(Either::Left(split_chunk)),
            None => {
                let (epoch, item) = self.inner.next_item().await?;
                match item {
                    LogStoreReadItem::StreamChunk { chunk, chunk_id } => {
                        Ok(Either::Left(SplitChunk {
                            chunk,
                            upstream_chunk_offset: UpstreamChunkOffset::Chunk { epoch, chunk_id },
                            is_last: true,
                        }))
                    }
                    LogStoreReadItem::Barrier { .. } => {
                        self.consumed_offset_queue.push_front((
                            TruncateOffset::Barrier { epoch },
                            TruncateOffset::Barrier { epoch },
                        ));
                        self.next_chunk_id = 0;
                        Ok(Either::Right((epoch, item)))
                    }
                }
            }
        }
    }
}

pub struct RateLimitedLogReader<R: LogReader> {
    core: RateLimitedLogReaderCore<R>,
    rate_limiter: RateLimiter,
    control_rx: UnboundedReceiver<RateLimit>,
}

impl<R: LogReader> RateLimitedLogReader<R> {
    pub fn new(inner: R, control_rx: UnboundedReceiver<RateLimit>) -> Self {
        Self {
            core: RateLimitedLogReaderCore {
                inner,
                consumed_offset_queue: VecDeque::new(),
                unconsumed_chunk_queue: VecDeque::new(),
                next_chunk_id: 0,
            },
            rate_limiter: RateLimiter::new(RateLimit::Disabled),
            control_rx,
        }
    }
}

impl<R: LogReader> RateLimitedLogReader<R> {
    async fn apply_rate_limit(
        &mut self,
        split_chunk: SplitChunk,
    ) -> LogStoreResult<(u64, LogStoreReadItem)> {
        let split_chunk = match self.rate_limiter.rate_limit() {
            RateLimit::Pause => unreachable!(
                "apply_rate_limit is not supposed to be called while the stream is paused"
            ),
            RateLimit::Disabled => split_chunk,
            RateLimit::Fixed(limit) => {
                let limit = limit.get();
                let required_permits = split_chunk.chunk.compute_rate_limit_chunk_permits();
                if required_permits <= limit {
                    self.rate_limiter.wait(required_permits).await;
                    split_chunk
                } else {
                    // Cut the chunk into smaller chunks
                    let mut chunks = split_chunk.chunk.split(limit as _).into_iter();
                    let mut is_last = split_chunk.is_last;
                    let upstream_chunk_offset = split_chunk.upstream_chunk_offset;

                    // The first chunk after splitting will be returned
                    let first_chunk = chunks.next().unwrap();

                    // The remaining chunks will be pushed to the queue
                    for chunk in chunks.rev() {
                        // The last chunk after splitting inherits the `is_last` from the original chunk
                        self.core.unconsumed_chunk_queue.push_back(SplitChunk {
                            chunk,
                            upstream_chunk_offset,
                            is_last,
                        });
                        is_last = false;
                    }

                    // Trigger rate limit and return the first chunk
                    self.rate_limiter
                        .wait(first_chunk.compute_rate_limit_chunk_permits())
                        .await;
                    SplitChunk {
                        chunk: first_chunk,
                        upstream_chunk_offset,
                        is_last,
                    }
                }
            }
        };

        // Update the consumed_offset_queue if the `split_chunk` is the last chunk of the upstream chunk
        let epoch = split_chunk.upstream_chunk_offset.epoch();
        let downstream_chunk_id = self.core.next_chunk_id;
        self.core.next_chunk_id += 1;
        if split_chunk.is_last {
            self.core.consumed_offset_queue.push_front((
                TruncateOffset::Chunk {
                    epoch,
                    chunk_id: downstream_chunk_id,
                },
                split_chunk.upstream_chunk_offset,
            ));
        }

        Ok((
            epoch,
            LogStoreReadItem::StreamChunk {
                chunk: split_chunk.chunk,
                chunk_id: downstream_chunk_id,
            },
        ))
    }
}

impl<R: LogReader> LogReader for RateLimitedLogReader<R> {
    async fn init(&mut self) -> LogStoreResult<()> {
        self.core.inner.init().await
    }

    async fn next_item(&mut self) -> LogStoreResult<(u64, LogStoreReadItem)> {
        let mut paused = false;
        loop {
            select! {
                biased;
                recv = pin!(self.control_rx.recv()) => {
                    let new_rate_limit = match recv {
                        Some(limit) => limit,
                        None => bail!("rate limit control channel closed"),
                    };
                    let old_rate_limit = self.rate_limiter.update(new_rate_limit);
                    paused = matches!(new_rate_limit, RateLimit::Pause);
                    tracing::info!("rate limit changed from {:?} to {:?}, paused = {paused}", old_rate_limit, new_rate_limit);
                },
                item = self.core.next_item(), if !paused => {
                    let item = item?;
                    match item {
                        Either::Left(split_chunk) => {
                            return self.apply_rate_limit(split_chunk).await;
                        },
                        Either::Right(item) => {
                            assert!(matches!(item.1, LogStoreReadItem::Barrier{..}));
                            return Ok(item);
                        },
                    }
                }
            }
        }
    }

    fn truncate(&mut self, offset: TruncateOffset) -> LogStoreResult<()> {
        let mut truncate_offset = None;
        while let Some((downstream_offset, upstream_offset)) =
            self.core.consumed_offset_queue.back()
        {
            if *downstream_offset <= offset {
                truncate_offset = Some(*upstream_offset);
                self.core.consumed_offset_queue.pop_back();
            } else {
                break;
            }
        }
        tracing::trace!(
            "rate limited log store reader truncate offset {:?}, downstream offset {:?}",
            truncate_offset,
            offset
        );
        if let Some(offset) = truncate_offset {
            self.core.inner.truncate(offset)
        } else {
            Ok(())
        }
    }

    fn rewind(&mut self) -> impl Future<Output = LogStoreResult<()>> + Send + '_ {
        self.core.unconsumed_chunk_queue.clear();
        self.core.consumed_offset_queue.clear();
        self.core.next_chunk_id = 0;
        self.core.inner.rewind()
    }

    fn start_from(
        &mut self,
        start_offset: Option<u64>,
    ) -> impl Future<Output = LogStoreResult<()>> + Send + '_ {
        self.core.inner.start_from(start_offset)
    }
}

#[easy_ext::ext(LogReaderExt)]
impl<T> T
where
    T: LogReader,
{
    pub fn transform_chunk<F: Fn(StreamChunk) -> StreamChunk + Sized>(
        self,
        f: F,
    ) -> TransformChunkLogReader<F, Self> {
        TransformChunkLogReader { f, inner: self }
    }

    pub fn monitored(self, metrics: LogReaderMetrics) -> impl LogReader {
        // TODO: The `clone()` can be avoided if move backpressure inside `MonitoredLogReader`
        let wait_new_future_duration = metrics.log_store_reader_wait_new_future_duration_ns.clone();
        BackpressureMonitoredLogReader::new(
            MonitoredLogReader::new(self, metrics),
            wait_new_future_duration,
        )
    }

    pub fn rate_limited(self, control_rx: UnboundedReceiver<RateLimit>) -> impl LogReader {
        RateLimitedLogReader::new(self, control_rx)
    }
}

pub struct MonitoredLogWriter<W: LogWriter> {
    inner: W,
    metrics: LogWriterMetrics,
}

pub struct LogWriterMetrics {
    // Labels: [actor_id, sink_id, sink_name]
    pub log_store_first_write_epoch: LabelGuardedIntGauge<3>,
    pub log_store_latest_write_epoch: LabelGuardedIntGauge<3>,
    pub log_store_write_rows: LabelGuardedIntCounter<3>,
}

impl<W: LogWriter> LogWriter for MonitoredLogWriter<W> {
    async fn init(
        &mut self,
        epoch: EpochPair,
        pause_read_on_bootstrap: bool,
    ) -> LogStoreResult<()> {
        self.metrics
            .log_store_first_write_epoch
            .set(epoch.curr as _);
        self.metrics
            .log_store_latest_write_epoch
            .set(epoch.curr as _);
        self.inner.init(epoch, pause_read_on_bootstrap).await
    }

    async fn write_chunk(&mut self, chunk: StreamChunk) -> LogStoreResult<()> {
        self.metrics
            .log_store_write_rows
            .inc_by(chunk.cardinality() as _);
        self.inner.write_chunk(chunk).await
    }

    async fn flush_current_epoch(
        &mut self,
        next_epoch: u64,
        options: FlushCurrentEpochOptions,
    ) -> LogStoreResult<LogWriterPostFlushCurrentEpoch<'_>> {
        let post_flush = self.inner.flush_current_epoch(next_epoch, options).await?;
        self.metrics
            .log_store_latest_write_epoch
            .set(next_epoch as _);
        Ok(post_flush)
    }

    fn pause(&mut self) -> LogStoreResult<()> {
        self.inner.pause()
    }

    fn resume(&mut self) -> LogStoreResult<()> {
        self.inner.resume()
    }
}

#[easy_ext::ext(LogWriterExt)]
impl<T> T
where
    T: LogWriter + Sized,
{
    pub fn monitored(self, metrics: LogWriterMetrics) -> MonitoredLogWriter<T> {
        MonitoredLogWriter {
            inner: self,
            metrics,
        }
    }
}

enum DeliveryFutureManagerItem<F> {
    Chunk {
        chunk_id: ChunkId,
        // earlier future at the front
        futures: VecDeque<F>,
    },
    Barrier,
}

pub struct DeliveryFutureManager<F> {
    future_count: usize,
    max_future_count: usize,
    // earlier items at the front
    items: VecDeque<(u64, DeliveryFutureManagerItem<F>)>,
}

impl<F> DeliveryFutureManager<F> {
    pub fn new(max_future_count: usize) -> Self {
        Self {
            future_count: 0,
            max_future_count,
            items: Default::default(),
        }
    }

    pub fn add_barrier(&mut self, epoch: u64) {
        if let Some((item_epoch, last_item)) = self.items.back() {
            match last_item {
                DeliveryFutureManagerItem::Chunk { .. } => {
                    assert_eq!(*item_epoch, epoch)
                }
                DeliveryFutureManagerItem::Barrier => {
                    assert!(
                        epoch > *item_epoch,
                        "new barrier epoch {} should be greater than prev barrier {}",
                        epoch,
                        item_epoch
                    );
                }
            }
        }
        self.items
            .push_back((epoch, DeliveryFutureManagerItem::Barrier));
    }

    pub fn start_write_chunk(
        &mut self,
        epoch: u64,
        chunk_id: ChunkId,
    ) -> DeliveryFutureManagerAddFuture<'_, F> {
        if let Some((item_epoch, item)) = self.items.back() {
            match item {
                DeliveryFutureManagerItem::Chunk {
                    chunk_id: item_chunk_id,
                    ..
                } => {
                    assert_eq!(epoch, *item_epoch);
                    assert!(
                        chunk_id > *item_chunk_id,
                        "new chunk id {} should be greater than prev chunk id {}",
                        chunk_id,
                        item_chunk_id
                    );
                }
                DeliveryFutureManagerItem::Barrier => {
                    assert!(
                        epoch > *item_epoch,
                        "new chunk epoch {} should be greater than prev barrier: {}",
                        epoch,
                        item_epoch
                    );
                }
            }
        }
        self.items.push_back((
            epoch,
            DeliveryFutureManagerItem::Chunk {
                chunk_id,
                futures: VecDeque::new(),
            },
        ));
        DeliveryFutureManagerAddFuture(self)
    }
}

pub struct DeliveryFutureManagerAddFuture<'a, F>(&'a mut DeliveryFutureManager<F>);

impl<F: TryFuture<Ok = ()> + Unpin + 'static> DeliveryFutureManagerAddFuture<'_, F> {
    /// Add a new future to the latest started written chunk.
    /// The returned bool value indicate whether we have awaited on any previous futures.
    pub async fn add_future_may_await(&mut self, future: F) -> Result<bool, F::Error> {
        let mut has_await = false;
        while self.0.future_count >= self.0.max_future_count {
            self.await_one_delivery().await?;
            has_await = true;
        }
        match self.0.items.back_mut() {
            Some((_, DeliveryFutureManagerItem::Chunk { futures, .. })) => {
                futures.push_back(future);
                self.0.future_count += 1;
                Ok(has_await)
            }
            _ => unreachable!("should add future only after add a new chunk"),
        }
    }

    pub async fn await_one_delivery(&mut self) -> Result<(), F::Error> {
        for (_, item) in &mut self.0.items {
            if let DeliveryFutureManagerItem::Chunk { futures, .. } = item
                && let Some(mut delivery_future) = futures.pop_front()
            {
                self.0.future_count -= 1;
                return poll_fn(|cx| delivery_future.try_poll_unpin(cx)).await;
            } else {
                continue;
            }
        }
        Ok(())
    }

    pub fn future_count(&self) -> usize {
        self.0.future_count
    }

    pub fn max_future_count(&self) -> usize {
        self.0.max_future_count
    }
}

impl<F: TryFuture<Ok = ()> + Unpin + 'static> DeliveryFutureManager<F> {
    pub fn next_truncate_offset(
        &mut self,
    ) -> impl Future<Output = Result<TruncateOffset, F::Error>> + '_ {
        poll_fn(move |cx| {
            let mut latest_offset: Option<TruncateOffset> = None;
            'outer: while let Some((epoch, item)) = self.items.front_mut() {
                match item {
                    DeliveryFutureManagerItem::Chunk { chunk_id, futures } => {
                        while let Some(future) = futures.front_mut() {
                            match future.try_poll_unpin(cx) {
                                Poll::Ready(result) => match result {
                                    Ok(()) => {
                                        self.future_count -= 1;
                                        futures.pop_front();
                                    }
                                    Err(e) => {
                                        return Poll::Ready(Err(e));
                                    }
                                },
                                Poll::Pending => {
                                    break 'outer;
                                }
                            }
                        }

                        // when we reach here, there must not be any pending or error future.
                        // Which means all futures of this stream chunk have been finished
                        assert!(futures.is_empty());
                        latest_offset = Some(TruncateOffset::Chunk {
                            epoch: *epoch,
                            chunk_id: *chunk_id,
                        });
                        self.items.pop_front().expect("items not empty");
                    }
                    DeliveryFutureManagerItem::Barrier => {
                        latest_offset = Some(TruncateOffset::Barrier { epoch: *epoch });
                        self.items.pop_front().expect("items not empty");
                        // Barrier will be yielded anyway
                        break 'outer;
                    }
                }
            }
            if let Some(offset) = latest_offset {
                Poll::Ready(Ok(offset))
            } else {
                Poll::Pending
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use std::future::{Future, poll_fn};
    use std::pin::pin;
    use std::task::Poll;

    use futures::{FutureExt, TryFuture};
    use risingwave_common::util::epoch::test_epoch;
    use tokio::sync::oneshot;
    use tokio::sync::oneshot::Receiver;

    use super::LogStoreResult;
    use crate::sink::log_store::{DeliveryFutureManager, TruncateOffset};

    #[test]
    fn test_truncate_offset_cmp() {
        assert!(
            TruncateOffset::Barrier { epoch: 232 }
                < TruncateOffset::Chunk {
                    epoch: 233,
                    chunk_id: 1
                }
        );
        assert_eq!(
            TruncateOffset::Chunk {
                epoch: 1,
                chunk_id: 1
            },
            TruncateOffset::Chunk {
                epoch: 1,
                chunk_id: 1
            }
        );
        assert!(
            TruncateOffset::Chunk {
                epoch: 1,
                chunk_id: 1
            } < TruncateOffset::Chunk {
                epoch: 1,
                chunk_id: 2
            }
        );
        assert!(
            TruncateOffset::Barrier { epoch: 1 }
                > TruncateOffset::Chunk {
                    epoch: 1,
                    chunk_id: 2
                }
        );
        assert!(
            TruncateOffset::Chunk {
                epoch: 1,
                chunk_id: 2
            } < TruncateOffset::Barrier { epoch: 1 }
        );
        assert!(
            TruncateOffset::Chunk {
                epoch: 2,
                chunk_id: 2
            } > TruncateOffset::Barrier { epoch: 1 }
        );
        assert!(TruncateOffset::Barrier { epoch: 2 } > TruncateOffset::Barrier { epoch: 1 });
    }

    type TestFuture = impl TryFuture<Ok = (), Error = anyhow::Error> + Unpin + 'static;
    fn to_test_future(rx: Receiver<LogStoreResult<()>>) -> TestFuture {
        async move { rx.await.unwrap() }.boxed()
    }

    #[tokio::test]
    async fn test_empty() {
        let mut manager = DeliveryFutureManager::<TestFuture>::new(2);
        let mut future = pin!(manager.next_truncate_offset());
        assert!(
            poll_fn(|cx| Poll::Ready(future.as_mut().poll(cx)))
                .await
                .is_pending()
        );
    }

    #[tokio::test]
    async fn test_future_delivery_manager_basic() {
        let mut manager = DeliveryFutureManager::new(2);
        let epoch1 = 233;
        let chunk_id1 = 1;
        let (tx1_1, rx1_1) = oneshot::channel();
        let mut write_chunk = manager.start_write_chunk(epoch1, chunk_id1);
        assert!(
            !write_chunk
                .add_future_may_await(to_test_future(rx1_1))
                .await
                .unwrap()
        );
        assert_eq!(manager.future_count, 1);
        {
            let mut next_truncate_offset = pin!(manager.next_truncate_offset());
            assert!(
                poll_fn(|cx| Poll::Ready(next_truncate_offset.as_mut().poll(cx)))
                    .await
                    .is_pending()
            );
            tx1_1.send(Ok(())).unwrap();
            assert_eq!(
                next_truncate_offset.await.unwrap(),
                TruncateOffset::Chunk {
                    epoch: epoch1,
                    chunk_id: chunk_id1
                }
            );
        }
        assert_eq!(manager.future_count, 0);
        manager.add_barrier(epoch1);
        assert_eq!(
            manager.next_truncate_offset().await.unwrap(),
            TruncateOffset::Barrier { epoch: epoch1 }
        );
    }

    #[tokio::test]
    async fn test_future_delivery_manager_compress_chunk() {
        let mut manager = DeliveryFutureManager::new(10);
        let epoch1 = test_epoch(233);
        let chunk_id1 = 1;
        let chunk_id2 = chunk_id1 + 1;
        let chunk_id3 = chunk_id2 + 1;
        let (tx1_1, rx1_1) = oneshot::channel();
        let (tx1_2, rx1_2) = oneshot::channel();
        let (tx1_3, rx1_3) = oneshot::channel();
        let epoch2 = test_epoch(234);
        let (tx2_1, rx2_1) = oneshot::channel();
        assert!(
            !manager
                .start_write_chunk(epoch1, chunk_id1)
                .add_future_may_await(to_test_future(rx1_1))
                .await
                .unwrap()
        );
        assert!(
            !manager
                .start_write_chunk(epoch1, chunk_id2)
                .add_future_may_await(to_test_future(rx1_2))
                .await
                .unwrap()
        );
        assert!(
            !manager
                .start_write_chunk(epoch1, chunk_id3)
                .add_future_may_await(to_test_future(rx1_3))
                .await
                .unwrap()
        );
        manager.add_barrier(epoch1);
        assert!(
            !manager
                .start_write_chunk(epoch2, chunk_id1)
                .add_future_may_await(to_test_future(rx2_1))
                .await
                .unwrap()
        );
        assert_eq!(manager.future_count, 4);
        {
            let mut next_truncate_offset = pin!(manager.next_truncate_offset());
            assert!(
                poll_fn(|cx| Poll::Ready(next_truncate_offset.as_mut().poll(cx)))
                    .await
                    .is_pending()
            );
            tx1_2.send(Ok(())).unwrap();
            assert!(
                poll_fn(|cx| Poll::Ready(next_truncate_offset.as_mut().poll(cx)))
                    .await
                    .is_pending()
            );
            tx1_1.send(Ok(())).unwrap();
            // The offset of chunk1 and chunk2 are compressed
            assert_eq!(
                next_truncate_offset.await.unwrap(),
                TruncateOffset::Chunk {
                    epoch: epoch1,
                    chunk_id: chunk_id2
                }
            );
        }
        assert_eq!(manager.future_count, 2);
        {
            let mut next_truncate_offset = pin!(manager.next_truncate_offset());
            assert!(
                poll_fn(|cx| Poll::Ready(next_truncate_offset.as_mut().poll(cx)))
                    .await
                    .is_pending()
            );
            tx1_3.send(Ok(())).unwrap();
            tx2_1.send(Ok(())).unwrap();
            // Emit barrier though later chunk has finished.
            assert_eq!(
                next_truncate_offset.await.unwrap(),
                TruncateOffset::Barrier { epoch: epoch1 }
            );
        }
        assert_eq!(manager.future_count, 1);
        assert_eq!(
            manager.next_truncate_offset().await.unwrap(),
            TruncateOffset::Chunk {
                epoch: epoch2,
                chunk_id: chunk_id1
            }
        );
    }

    #[tokio::test]
    async fn test_future_delivery_manager_await_future() {
        let mut manager = DeliveryFutureManager::new(2);
        let epoch = 233;
        let chunk_id1 = 1;
        let chunk_id2 = chunk_id1 + 1;
        let (tx1_1, rx1_1) = oneshot::channel();
        let (tx1_2, rx1_2) = oneshot::channel();
        let (tx2_1, rx2_1) = oneshot::channel();
        let (tx2_2, rx2_2) = oneshot::channel();

        {
            let mut write_chunk = manager.start_write_chunk(epoch, chunk_id1);
            assert!(
                !write_chunk
                    .add_future_may_await(to_test_future(rx1_1))
                    .await
                    .unwrap()
            );
            assert!(
                !write_chunk
                    .add_future_may_await(to_test_future(rx1_2))
                    .await
                    .unwrap()
            );
            assert_eq!(manager.future_count, 2);
        }

        {
            let mut write_chunk = manager.start_write_chunk(epoch, chunk_id2);
            {
                let mut future1 = pin!(write_chunk.add_future_may_await(to_test_future(rx2_1)));
                assert!(
                    poll_fn(|cx| Poll::Ready(future1.as_mut().poll(cx)))
                        .await
                        .is_pending()
                );
                tx1_1.send(Ok(())).unwrap();
                assert!(future1.await.unwrap());
            }
            assert_eq!(2, write_chunk.future_count());
            {
                let mut future2 = pin!(write_chunk.add_future_may_await(to_test_future(rx2_2)));
                assert!(
                    poll_fn(|cx| Poll::Ready(future2.as_mut().poll(cx)))
                        .await
                        .is_pending()
                );
                tx1_2.send(Ok(())).unwrap();
                assert!(future2.await.unwrap());
            }
            assert_eq!(2, write_chunk.future_count());
            {
                let mut future3 = pin!(write_chunk.await_one_delivery());
                assert!(
                    poll_fn(|cx| Poll::Ready(future3.as_mut().poll(cx)))
                        .await
                        .is_pending()
                );
                tx2_1.send(Ok(())).unwrap();
                future3.await.unwrap();
            }
            assert_eq!(1, write_chunk.future_count());
        }

        assert_eq!(
            manager.next_truncate_offset().await.unwrap(),
            TruncateOffset::Chunk {
                epoch,
                chunk_id: chunk_id1
            }
        );

        assert_eq!(1, manager.future_count);

        {
            let mut future = pin!(manager.next_truncate_offset());
            assert!(
                poll_fn(|cx| Poll::Ready(future.as_mut().poll(cx)))
                    .await
                    .is_pending()
            );
            tx2_2.send(Ok(())).unwrap();
            assert_eq!(
                future.await.unwrap(),
                TruncateOffset::Chunk {
                    epoch,
                    chunk_id: chunk_id2
                }
            );
        }

        assert_eq!(0, manager.future_count);
    }
}
