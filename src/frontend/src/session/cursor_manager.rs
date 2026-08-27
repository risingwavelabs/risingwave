// Copyright 2024 RisingWave Labs
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

use core::time::Duration;
use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt::{Display, Formatter};
use std::mem;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Instant;

use anyhow::anyhow;
use bytes::Bytes;
use futures::{Stream, StreamExt};
use itertools::Itertools;
use pgwire::pg_field_descriptor::PgFieldDescriptor;
use pgwire::pg_response::StatementType;
use pgwire::types::{Format, FormatIterator, Row};
use risingwave_batch::task::{ShutdownSender, ShutdownToken};
use risingwave_common::array::DataChunk;
use risingwave_common::catalog::{ColumnCatalog, Field};
use risingwave_common::error::BoxedError;
use risingwave_common::session_config::QueryMode;
use risingwave_common::types::{DataType, ScalarImpl, StructType, StructValue};
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_hummock_sdk::HummockVersionId;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

use super::SessionImpl;
use crate::catalog::TableId;
use crate::catalog::subscription_catalog::SubscriptionCatalog;
use crate::error::{ErrorCode, Result};
use crate::expr::{ExprType, FunctionCall, InputRef, Literal};
use crate::handler::HandlerArgs;
use crate::handler::query::{
    BatchPlanFragmenterResult, RwBatchQueryPlanResult, gen_batch_plan_fragmenter,
};
use crate::handler::util::{
    StaticSessionData, convert_logstore_u64_to_unix_millis, pg_value_format, to_pg_field,
    to_pg_rows,
};
use crate::monitor::{CursorMetrics, PeriodicCursorMetrics};
use crate::optimizer::PlanRoot;
use crate::optimizer::plan_node::{BatchFilter, BatchLogSeqScan, BatchSeqScan, generic};
use crate::optimizer::property::{Order, RequiredDist};
use crate::scheduler::plan_fragmenter::QueryId;
use crate::scheduler::{
    DistributedQueryStream, LocalQueryStream, QueryManager, ReadSnapshot, SchedulerError,
};
use crate::utils::Condition;
use crate::{OptimizerContext, OptimizerContextRef, TableCatalog};

/// Cancellation and buffering resources shared by one cursor's foreground and producer tasks.
struct CursorLifecycle {
    shutdown_tx: ShutdownSender,
    shutdown_rx: ShutdownToken,
    session_shutdown_rx: ShutdownToken,
    data_chunk_channel_capacity: usize,
}

impl CursorLifecycle {
    /// Creates lifecycle resources for a cursor in the given session.
    fn new(session_shutdown_rx: ShutdownToken, data_chunk_channel_capacity: usize) -> Self {
        let (shutdown_tx, shutdown_rx) = ShutdownToken::new();
        Self {
            shutdown_tx,
            shutdown_rx,
            session_shutdown_rx,
            data_chunk_channel_capacity,
        }
    }

    /// Returns a cursor-scoped token for cancelling an underlying query executor.
    fn query_shutdown_token(&self) -> ShutdownToken {
        self.shutdown_rx.clone()
    }

    /// Returns a cursor-scoped sender for cancelling an underlying local query executor.
    fn query_shutdown_sender(&self) -> ShutdownSender {
        self.shutdown_tx.clone()
    }

    /// Returns foreground-owned clones of the cursor and session shutdown tokens.
    fn shutdown_tokens(&self) -> (ShutdownToken, ShutdownToken) {
        (self.shutdown_rx.clone(), self.session_shutdown_rx.clone())
    }

    /// Signals this cursor's producer and underlying query executor to stop cooperatively.
    fn shutdown(&self) {
        self.shutdown_tx.cancel();
    }
}

impl Drop for CursorLifecycle {
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// The local or distributed query stream owned by a cursor producer.
enum CursorQueryStreamInner {
    /// A query executed by the frontend's local batch executor.
    Local {
        stream: LocalQueryStream,
        shutdown_tx: ShutdownSender,
    },
    /// A query scheduled through the distributed query manager.
    Distributed {
        stream: DistributedQueryStream,
        query_manager: QueryManager,
        query_id: QueryId,
    },
}

/// A cursor-owned query stream that cancels unfinished execution when dropped.
pub struct CursorQueryStream {
    inner: CursorQueryStreamInner,
    finished: bool,
}

impl CursorQueryStream {
    /// Wraps a local query stream and its cursor-scoped cancellation sender.
    pub fn local(stream: LocalQueryStream, shutdown_tx: ShutdownSender) -> Self {
        Self {
            inner: CursorQueryStreamInner::Local {
                stream,
                shutdown_tx,
            },
            finished: false,
        }
    }

    /// Wraps a distributed query stream and remembers how to cancel it when the cursor closes.
    pub fn distributed(stream: DistributedQueryStream, query_manager: QueryManager) -> Self {
        let query_id = stream.query_id().clone();
        Self {
            inner: CursorQueryStreamInner::Distributed {
                stream,
                query_manager,
                query_id,
            },
            finished: false,
        }
    }
}

impl Stream for CursorQueryStream {
    type Item = std::result::Result<DataChunk, BoxedError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let result = match &mut this.inner {
            CursorQueryStreamInner::Local { stream, .. } => stream.poll_next_unpin(cx),
            CursorQueryStreamInner::Distributed { stream, .. } => stream.poll_next_unpin(cx),
        };
        if matches!(&result, Poll::Ready(None)) {
            this.finished = true;
        }
        result
    }
}

impl Drop for CursorQueryStream {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        match &self.inner {
            CursorQueryStreamInner::Local { shutdown_tx, .. } => {
                shutdown_tx.cancel();
            }
            CursorQueryStreamInner::Distributed {
                query_manager,
                query_id,
                ..
            } => query_manager.cancel_query(query_id, "cursor closed"),
        }
    }
}

#[derive(Clone)]
/// Metadata needed to format a raw data chunk for PostgreSQL.
enum CursorDataChunkKind {
    /// A chunk from a regular query cursor.
    Query { fields: Arc<Vec<Field>> },
    /// A chunk from either the snapshot or log-store phase of a subscription cursor.
    Subscription {
        fields: Arc<FieldsManager>,
        from_snapshot: bool,
        rw_timestamp: u64,
    },
}

#[derive(Clone)]
/// One raw chunk produced independently of any PostgreSQL `FETCH` format.
struct CursorDataChunk {
    chunk: DataChunk,
    kind: CursorDataChunkKind,
}

#[derive(Clone)]
/// A non-row event that separates phases of cursor production.
enum CursorDataChunkBarrier {
    /// The only query owned by a regular query cursor has completed.
    QueryEnd,
    /// A subscription query has started and exposes a new producer state to the foreground.
    SubscriptionQueryStarted {
        /// The producer state corresponding to the newly started query.
        state: SubscriptionCursorState,
        /// The output fields for chunks produced by the query.
        output_fields: Vec<Field>,
        /// The time at which this subscription query's retained data is no longer valid.
        expires_at: Instant,
    },
    /// A subscription query completed and the producer advanced to the supplied next position.
    SubscriptionBatch {
        /// The next log-store position the producer will inspect.
        next_state: SubscriptionCursorState,
    },
    /// No subscription log-store epoch is currently available.
    SubscriptionIdle,
    /// The upstream table schema changed before the next subscription query began.
    SchemaChanged,
}

#[derive(Clone)]
/// A raw chunk or producer control barrier sent to the cursor foreground.
enum CursorDataChunkEvent {
    /// A data chunk whose rows can be formatted by the current `FETCH`.
    Chunk(CursorDataChunk),
    /// A control barrier produced between data chunks.
    Barrier(CursorDataChunkBarrier),
}

/// A producer-backed channel shared by concrete query and subscription data streams.
struct CursorDataChunkReceiver {
    /// Receives raw chunks and producer barriers without PostgreSQL row formatting.
    event_rx: mpsc::Receiver<std::result::Result<CursorDataChunkEvent, BoxedError>>,
    /// Completes after the producer observes lifecycle shutdown or reaches a terminal event.
    producer_handle: Option<JoinHandle<()>>,
}

impl Drop for CursorDataChunkReceiver {
    fn drop(&mut self) {
        let Some(producer_handle) = self.producer_handle.take() else {
            return;
        };
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                if let Err(error) = producer_handle.await {
                    tracing::warn!(%error, "cursor producer task failed during shutdown");
                }
            });
        }
    }
}

/// A regular query cursor's immediately started raw data stream.
struct QueryCursorDataChunkStream {
    /// Shared channel and producer-task teardown resources.
    receiver: CursorDataChunkReceiver,
}

impl QueryCursorDataChunkStream {
    /// Starts a producer for an already-created regular query cursor.
    fn new(
        mut query_stream: CursorQueryStream,
        fields: Vec<Field>,
        lifecycle: &CursorLifecycle,
    ) -> Self {
        let (event_tx, event_rx) = mpsc::channel(lifecycle.data_chunk_channel_capacity);
        let mut shutdown_rx = lifecycle.shutdown_rx.clone();
        let mut session_shutdown_rx = lifecycle.session_shutdown_rx.clone();
        let query_fields = Arc::new(fields);
        let producer_handle = tokio::spawn(async move {
            loop {
                let next = tokio::select! {
                    biased;
                    _ = shutdown_rx.cancelled() => break,
                    _ = session_shutdown_rx.cancelled() => break,
                    next = query_stream.next() => next,
                };
                let event = match next {
                    Some(Ok(chunk)) => Ok(CursorDataChunkEvent::Chunk(CursorDataChunk {
                        chunk,
                        kind: CursorDataChunkKind::Query {
                            fields: query_fields.clone(),
                        },
                    })),
                    Some(Err(error)) => Err(error),
                    None => Ok(CursorDataChunkEvent::Barrier(
                        CursorDataChunkBarrier::QueryEnd,
                    )),
                };
                let terminal = !matches!(&event, Ok(CursorDataChunkEvent::Chunk(_)));
                let sent = tokio::select! {
                    biased;
                    _ = shutdown_rx.cancelled() => false,
                    _ = session_shutdown_rx.cancelled() => false,
                    result = event_tx.send(event) => result.is_ok(),
                };
                if terminal || !sent {
                    break;
                }
            }
        });
        Self {
            receiver: CursorDataChunkReceiver {
                event_rx,
                producer_handle: Some(producer_handle),
            },
        }
    }
}

impl Stream for QueryCursorDataChunkStream {
    type Item = std::result::Result<CursorDataChunkEvent, BoxedError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.get_mut().receiver.event_rx.poll_recv(cx)
    }
}

/// A subscription cursor's deferred raw data stream.
struct SubscriptionCursorDataChunkStream {
    /// Shared channel and producer-task teardown resources.
    receiver: CursorDataChunkReceiver,
    /// Releases the deferred subscription producer on the first `FETCH`.
    start_tx: Option<oneshot::Sender<()>>,
}

impl SubscriptionCursorDataChunkStream {
    /// Starts a deferred subscription producer when a subscription cursor's first `FETCH` begins.
    fn new(mut producer: SubscriptionDataChunkProducer, lifecycle: &CursorLifecycle) -> Self {
        let (event_tx, event_rx) = mpsc::channel(lifecycle.data_chunk_channel_capacity);
        let (start_tx, start_rx) = oneshot::channel();
        let mut shutdown_rx = lifecycle.shutdown_rx.clone();
        let mut session_shutdown_rx = lifecycle.session_shutdown_rx.clone();
        let producer_handle = tokio::spawn(async move {
            tokio::select! {
                biased;
                _ = shutdown_rx.cancelled() => return,
                _ = session_shutdown_rx.cancelled() => return,
                _ = start_rx => {}
            }
            producer
                .run(event_tx, &mut shutdown_rx, &mut session_shutdown_rx)
                .await;
        });
        Self {
            receiver: CursorDataChunkReceiver {
                event_rx,
                producer_handle: Some(producer_handle),
            },
            start_tx: Some(start_tx),
        }
    }

    /// Starts this subscription producer exactly once.
    fn start(&mut self) {
        if let Some(start_tx) = self.start_tx.take() {
            _ = start_tx.send(());
        }
    }
}

impl Stream for SubscriptionCursorDataChunkStream {
    type Item = std::result::Result<CursorDataChunkEvent, BoxedError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.get_mut().receiver.event_rx.poll_recv(cx)
    }
}

/// A raw event retained until a successful `FETCH` commits past it.
struct CachedCursorDataChunkEvent {
    event: CursorDataChunkEvent,
    row_offset: usize,
}

/// Rows formatted for the current `FETCH` and their next raw-chunk offset.
struct FormattedCursorDataChunk {
    next_offset: usize,
    rows: VecDeque<Row>,
}

/// Tentative row and raw-event progress shared by every kind of `FETCH` command.
struct CursorPgResponseFetchStateCommon {
    formats: Vec<Format>,
    session_data: StaticSessionData,
    next_event_index: usize,
    current_chunk: Option<FormattedCursorDataChunk>,
    yielded_rows: usize,
    finished: bool,
}

impl CursorPgResponseFetchStateCommon {
    /// Creates uncommitted progress for a new `FETCH` command.
    fn new(formats: &[Format], session: &SessionImpl) -> Self {
        Self {
            formats: formats.to_vec(),
            session_data: StaticSessionData {
                timezone: session.config().timezone(),
            },
            next_event_index: 0,
            current_chunk: None,
            yielded_rows: 0,
            finished: false,
        }
    }
}

/// Query- or subscription-specific tentative state for the active `FETCH` command.
enum CursorPgResponseFetchState {
    /// Tentative progress for a regular query cursor.
    Query {
        /// Row and raw-event progress shared with subscription cursors.
        common: CursorPgResponseFetchStateCommon,
    },
    /// Tentative progress and producer metadata for a subscription cursor.
    Subscription {
        /// Row and raw-event progress shared with regular query cursors.
        common: CursorPgResponseFetchStateCommon,
        /// Whether an idle `FETCH` waits for the producer to receive future data.
        wait_for_data: bool,
        /// Producer position to commit after this `FETCH` succeeds.
        next_subscription_state: Option<SubscriptionCursorState>,
        /// Output fields to commit after this `FETCH` succeeds.
        next_output_fields: Option<Vec<Field>>,
        /// Retention deadline to commit after this `FETCH` succeeds.
        next_expires_at: Option<Instant>,
        /// Producer-idle status to commit after this `FETCH` succeeds.
        next_subscription_idle: Option<bool>,
    },
}

impl CursorPgResponseFetchState {
    /// Returns the tentative progress shared by both cursor kinds.
    fn common(&self) -> &CursorPgResponseFetchStateCommon {
        match self {
            Self::Query { common } | Self::Subscription { common, .. } => common,
        }
    }

    /// Returns mutable tentative progress shared by both cursor kinds.
    fn common_mut(&mut self) -> &mut CursorPgResponseFetchStateCommon {
        match self {
            Self::Query { common } | Self::Subscription { common, .. } => common,
        }
    }

    /// Returns whether an idle subscription `FETCH` should wait for newly produced data.
    fn wait_for_data(&self) -> bool {
        match self {
            Self::Query { .. } => false,
            Self::Subscription { wait_for_data, .. } => *wait_for_data,
        }
    }

    /// Records the subscription-idle status to commit after a successful `FETCH`.
    fn set_next_subscription_idle(&mut self, idle: bool) {
        match self {
            Self::Query { .. } => unreachable!("query fetch cannot update subscription state"),
            Self::Subscription {
                next_subscription_idle,
                ..
            } => *next_subscription_idle = Some(idle),
        }
    }

    /// Records metadata for a newly started subscription query.
    fn set_subscription_query_started(
        &mut self,
        state: SubscriptionCursorState,
        output_fields: Vec<Field>,
        expires_at: Instant,
    ) {
        match self {
            Self::Query { .. } => unreachable!("query fetch cannot update subscription state"),
            Self::Subscription {
                next_subscription_state,
                next_output_fields,
                next_expires_at,
                ..
            } => {
                *next_subscription_state = Some(state);
                *next_output_fields = Some(output_fields);
                *next_expires_at = Some(expires_at);
            }
        }
    }

    /// Records the next subscription log-store position.
    fn set_next_subscription_state(&mut self, state: SubscriptionCursorState) {
        match self {
            Self::Query { .. } => unreachable!("query fetch cannot update subscription state"),
            Self::Subscription {
                next_subscription_state,
                ..
            } => *next_subscription_state = Some(state),
        }
    }
}

/// State shared by concrete query and subscription PostgreSQL response streams.
struct CursorPgResponseStreamCommon<S> {
    data_stream: S,
    cached_events: VecDeque<CachedCursorDataChunkEvent>,
    fetch_state: Option<CursorPgResponseFetchState>,
    output_fields: Vec<Field>,
    failed: bool,
}

/// One item produced by the shared response-stream polling core.
enum CursorPgResponsePollItem {
    /// A formatted PostgreSQL row.
    Row(Row),
    /// A producer control barrier interpreted by the concrete response stream.
    Barrier(CursorDataChunkBarrier),
    /// The underlying cursor producer channel closed.
    DataStreamEnd,
}

impl<S> CursorPgResponseStreamCommon<S> {
    /// Creates shared response-stream state around one concrete raw-event stream.
    fn new(data_stream: S, output_fields: Vec<Field>) -> Self {
        Self {
            data_stream,
            cached_events: VecDeque::new(),
            fetch_state: None,
            output_fields,
            failed: false,
        }
    }

    /// Returns the output fields committed by previous `FETCH` commands.
    fn fields(&self) -> Vec<Field> {
        self.output_fields.clone()
    }

    /// Returns whether the producer reported a terminal internal failure through the channel.
    fn is_failed(&self) -> bool {
        self.failed
    }

    /// Returns whether the active `FETCH` has reached its command boundary.
    fn fetch_finished(&self) -> bool {
        self.fetch_state
            .as_ref()
            .is_some_and(|state| state.common().finished)
    }

    /// Marks the active `FETCH` as complete without committing its tentative position yet.
    fn finish_fetch(&mut self) {
        self.fetch_state
            .as_mut()
            .expect("response stream must be inside a FETCH")
            .common_mut()
            .finished = true;
    }

    /// Returns the active `FETCH` state.
    fn fetch_state(&self) -> &CursorPgResponseFetchState {
        self.fetch_state
            .as_ref()
            .expect("response stream must be inside a FETCH")
    }

    /// Returns mutable active `FETCH` state.
    fn fetch_state_mut(&mut self) -> &mut CursorPgResponseFetchState {
        self.fetch_state
            .as_mut()
            .expect("response stream must be inside a FETCH")
    }

    /// Commits raw-event progress and returns cursor-kind-specific metadata to its owner.
    fn commit_fetch_state(&mut self) -> Option<CursorPgResponseFetchState> {
        let mut fetch_state = self.fetch_state.take()?;
        let next_event_index = {
            let common = fetch_state.common_mut();
            if let Some(chunk) = common.current_chunk.take() {
                if chunk.rows.is_empty() {
                    common.next_event_index += 1;
                } else {
                    let event = self
                        .cached_events
                        .get_mut(common.next_event_index)
                        .expect("current formatted chunk must have a cached raw chunk");
                    debug_assert!(matches!(event.event, CursorDataChunkEvent::Chunk(_)));
                    event.row_offset = chunk.next_offset;
                }
            }
            common.next_event_index
        };
        drop(self.cached_events.drain(..next_event_index));
        Some(fetch_state)
    }

    /// Rolls back the current `FETCH` while retaining every raw event for the next command.
    fn abort_fetch(&mut self) {
        self.fetch_state = None;
    }

    /// Formats a raw query or subscription chunk for one PostgreSQL `FETCH`.
    fn format_chunk(
        data: &CursorDataChunk,
        formats: &[Format],
        session_data: &StaticSessionData,
    ) -> Result<Vec<Row>> {
        match &data.kind {
            CursorDataChunkKind::Query { fields } => {
                let column_types = fields.iter().map(|field| field.data_type()).collect_vec();
                to_pg_rows(&column_types, data.chunk.clone(), formats, session_data)
            }
            CursorDataChunkKind::Subscription {
                fields,
                from_snapshot,
                rw_timestamp,
            } => {
                let (row_fields, row_formats) =
                    fields.get_row_stream_fields_and_formats(formats, *from_snapshot)?;
                let column_types = row_fields
                    .iter()
                    .map(|field| field.data_type())
                    .collect_vec();
                let raw_formats = if row_formats.is_empty() {
                    &[][..]
                } else {
                    &row_formats[..column_types.len()]
                };
                to_pg_rows(&column_types, data.chunk.clone(), raw_formats, session_data)?
                    .into_iter()
                    .map(|row| {
                        let mut row = SubscriptionCursor::build_row(
                            row.take(),
                            (!*from_snapshot).then_some(*rw_timestamp),
                            &row_formats,
                            session_data,
                        )?;
                        Ok(row.project(&fields.row_output_col_indices))
                    })
                    .try_collect()
            }
        }
    }
}

impl<S> CursorPgResponseStreamCommon<S>
where
    S: Stream<Item = std::result::Result<CursorDataChunkEvent, BoxedError>> + Unpin,
{
    /// Polls one formatted row, control barrier, or producer-channel termination.
    fn poll_next_item(&mut self, cx: &mut Context<'_>) -> Poll<Result<CursorPgResponsePollItem>> {
        loop {
            let Some(fetch_state) = self.fetch_state.as_mut() else {
                return Poll::Ready(Err(ErrorCode::InternalError(
                    "cursor response stream polled outside a FETCH".to_owned(),
                )
                .into()));
            };

            {
                let common = fetch_state.common_mut();
                if let Some(chunk) = common.current_chunk.as_mut()
                    && let Some(row) = chunk.rows.pop_front()
                {
                    chunk.next_offset += 1;
                    common.yielded_rows += 1;
                    return Poll::Ready(Ok(CursorPgResponsePollItem::Row(row)));
                }
            }

            if fetch_state.common_mut().current_chunk.take().is_some() {
                fetch_state.common_mut().next_event_index += 1;
                continue;
            }

            let event_index = fetch_state.common().next_event_index;
            if event_index == self.cached_events.len() {
                match self.data_stream.poll_next_unpin(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Some(Ok(event))) => {
                        self.cached_events.push_back(CachedCursorDataChunkEvent {
                            event,
                            row_offset: 0,
                        });
                        continue;
                    }
                    Poll::Ready(Some(Err(error))) => {
                        self.failed = true;
                        fetch_state.common_mut().finished = true;
                        return Poll::Ready(Err(error.into()));
                    }
                    Poll::Ready(None) => {
                        fetch_state.common_mut().finished = true;
                        return Poll::Ready(Ok(CursorPgResponsePollItem::DataStreamEnd));
                    }
                }
            }
            debug_assert!(event_index < self.cached_events.len());
            let event = self.cached_events[event_index].event.clone();
            let row_offset = self.cached_events[event_index].row_offset;

            match event {
                CursorDataChunkEvent::Chunk(data) => {
                    let rows = match Self::format_chunk(
                        &data,
                        &fetch_state.common().formats,
                        &fetch_state.common().session_data,
                    ) {
                        Ok(rows) => rows,
                        Err(error) => {
                            return Poll::Ready(Err(error));
                        }
                    };
                    fetch_state.common_mut().current_chunk = Some(FormattedCursorDataChunk {
                        next_offset: row_offset,
                        rows: rows.into_iter().skip(row_offset).collect(),
                    });
                }
                CursorDataChunkEvent::Barrier(barrier) => {
                    fetch_state.common_mut().next_event_index += 1;
                    return Poll::Ready(Ok(CursorPgResponsePollItem::Barrier(barrier)));
                }
            }
        }
    }
}

/// A regular query cursor's rollback-safe PostgreSQL response stream.
struct QueryCursorPgResponseStream {
    common: CursorPgResponseStreamCommon<QueryCursorDataChunkStream>,
}

impl QueryCursorPgResponseStream {
    /// Creates the foreground stream for a regular query cursor.
    fn new(data_stream: QueryCursorDataChunkStream, output_fields: Vec<Field>) -> Self {
        Self {
            common: CursorPgResponseStreamCommon::new(data_stream, output_fields),
        }
    }

    /// Returns the output fields committed by previous `FETCH` commands.
    fn fields(&self) -> Vec<Field> {
        self.common.fields()
    }

    /// Begins tentative progress for one regular query `FETCH` command.
    fn begin_fetch(&mut self, formats: &[Format], session: &SessionImpl) {
        self.common.fetch_state = Some(CursorPgResponseFetchState::Query {
            common: CursorPgResponseFetchStateCommon::new(formats, session),
        });
    }

    /// Commits the current `FETCH` position.
    fn commit_fetch(&mut self) {
        _ = self.common.commit_fetch_state();
    }

    /// Rolls back the current `FETCH` while retaining every raw event for the next command.
    fn abort_fetch(&mut self) {
        self.common.abort_fetch();
    }
}

impl Stream for QueryCursorPgResponseStream {
    type Item = Result<Row>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.common.fetch_finished() {
            return Poll::Ready(None);
        }
        match this.common.poll_next_item(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(error)) => Poll::Ready(Some(Err(error))),
            Poll::Ready(Ok(CursorPgResponsePollItem::Row(row))) => Poll::Ready(Some(Ok(row))),
            Poll::Ready(Ok(CursorPgResponsePollItem::Barrier(
                CursorDataChunkBarrier::QueryEnd | CursorDataChunkBarrier::SchemaChanged,
            ))) => {
                this.common.finish_fetch();
                Poll::Ready(None)
            }
            Poll::Ready(Ok(CursorPgResponsePollItem::Barrier(_))) => {
                this.common.finish_fetch();
                Poll::Ready(Some(Err(ErrorCode::InternalError(
                    "query cursor received a subscription producer barrier".to_owned(),
                )
                .into())))
            }
            Poll::Ready(Ok(CursorPgResponsePollItem::DataStreamEnd)) => {
                if this.common.failed {
                    Poll::Ready(Some(Err(ErrorCode::InternalError(
                        "Cursor data stream has terminated with an error; close and recreate the cursor"
                            .to_owned(),
                    )
                    .into())))
                } else {
                    Poll::Ready(None)
                }
            }
        }
    }
}

/// A subscription cursor's rollback-safe PostgreSQL response stream and committed metadata.
struct SubscriptionCursorPgResponseStream {
    common: CursorPgResponseStreamCommon<SubscriptionCursorDataChunkStream>,
    subscription_state: SubscriptionCursorState,
    expires_at: Instant,
    subscription_idle: bool,
}

impl SubscriptionCursorPgResponseStream {
    /// Creates the foreground stream for a subscription cursor.
    fn new(
        data_stream: SubscriptionCursorDataChunkStream,
        output_fields: Vec<Field>,
        subscription_state: SubscriptionCursorState,
        expires_at: Instant,
    ) -> Self {
        Self {
            common: CursorPgResponseStreamCommon::new(data_stream, output_fields),
            subscription_state,
            expires_at,
            subscription_idle: false,
        }
    }

    /// Returns the output fields committed by previous `FETCH` commands.
    fn fields(&self) -> Vec<Field> {
        self.common.fields()
    }

    /// Returns the subscription state committed by the foreground stream.
    fn subscription_state(&self) -> &SubscriptionCursorState {
        &self.subscription_state
    }

    /// Returns whether the subscription's current logical position has expired.
    fn is_expired(&self, now: Instant) -> bool {
        now > self.expires_at
    }

    /// Returns whether the producer reported a terminal internal failure through the channel.
    fn is_failed(&self) -> bool {
        self.common.is_failed()
    }

    /// Starts deferred production and begins tentative subscription `FETCH` progress.
    fn begin_fetch(&mut self, formats: &[Format], session: &SessionImpl, wait_for_data: bool) {
        self.common.fetch_state = Some(CursorPgResponseFetchState::Subscription {
            common: CursorPgResponseFetchStateCommon::new(formats, session),
            wait_for_data,
            next_subscription_state: None,
            next_output_fields: None,
            next_expires_at: None,
            next_subscription_idle: None,
        });
        self.common.data_stream.start();
    }

    /// Commits the current `FETCH` position and its ordered producer metadata.
    fn commit_fetch(&mut self) {
        let Some(fetch_state) = self.common.commit_fetch_state() else {
            return;
        };
        let CursorPgResponseFetchState::Subscription {
            next_subscription_state,
            next_output_fields,
            next_expires_at,
            next_subscription_idle,
            ..
        } = fetch_state
        else {
            unreachable!("subscription response stream must own subscription fetch state");
        };
        if let Some(state) = next_subscription_state {
            self.subscription_state = state;
        }
        if let Some(output_fields) = next_output_fields {
            self.common.output_fields = output_fields;
        }
        if let Some(expires_at) = next_expires_at {
            self.expires_at = expires_at;
        }
        if let Some(subscription_idle) = next_subscription_idle {
            self.subscription_idle = subscription_idle;
        }
    }

    /// Rolls back the current `FETCH` while retaining every raw event for the next command.
    fn abort_fetch(&mut self) {
        self.common.abort_fetch();
    }
}

impl Stream for SubscriptionCursorPgResponseStream {
    type Item = Result<Row>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            if this.common.fetch_finished() {
                return Poll::Ready(None);
            }
            match this.common.poll_next_item(cx) {
                Poll::Pending if this.subscription_idle => {
                    let fetch_state = this.common.fetch_state();
                    if fetch_state.common().yielded_rows > 0 || !fetch_state.wait_for_data() {
                        this.common.finish_fetch();
                        return Poll::Ready(None);
                    }
                    return Poll::Pending;
                }
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(error)) => {
                    if this.common.failed {
                        this.subscription_state = SubscriptionCursorState::Invalid;
                    }
                    return Poll::Ready(Some(Err(error)));
                }
                Poll::Ready(Ok(CursorPgResponsePollItem::Row(row))) => {
                    if this.subscription_idle {
                        this.common
                            .fetch_state_mut()
                            .set_next_subscription_idle(false);
                    }
                    return Poll::Ready(Some(Ok(row)));
                }
                Poll::Ready(Ok(CursorPgResponsePollItem::Barrier(barrier))) => {
                    if this.subscription_idle {
                        this.common
                            .fetch_state_mut()
                            .set_next_subscription_idle(false);
                    }
                    let should_finish = match barrier {
                        CursorDataChunkBarrier::QueryEnd
                        | CursorDataChunkBarrier::SchemaChanged => true,
                        CursorDataChunkBarrier::SubscriptionQueryStarted {
                            state,
                            output_fields,
                            expires_at,
                        } => {
                            this.common
                                .fetch_state_mut()
                                .set_subscription_query_started(state, output_fields, expires_at);
                            false
                        }
                        CursorDataChunkBarrier::SubscriptionBatch { next_state } => {
                            this.common
                                .fetch_state_mut()
                                .set_next_subscription_state(next_state);
                            this.common.fetch_state().common().yielded_rows > 0
                        }
                        CursorDataChunkBarrier::SubscriptionIdle => {
                            this.common
                                .fetch_state_mut()
                                .set_next_subscription_idle(true);
                            let fetch_state = this.common.fetch_state();
                            fetch_state.common().yielded_rows > 0 || !fetch_state.wait_for_data()
                        }
                    };
                    if should_finish {
                        this.common.finish_fetch();
                        return Poll::Ready(None);
                    }
                }
                Poll::Ready(Ok(CursorPgResponsePollItem::DataStreamEnd)) => {
                    if this.common.failed {
                        return Poll::Ready(Some(Err(ErrorCode::InternalError(
                            "Cursor data stream has terminated with an error; close and recreate the cursor"
                                .to_owned(),
                        )
                        .into())));
                    }
                    this.common.failed = true;
                    this.subscription_state = SubscriptionCursorState::Invalid;
                    return Poll::Ready(Some(Err(ErrorCode::InternalError(
                        "Subscription cursor producer terminated unexpectedly; close and recreate the cursor"
                            .to_owned(),
                    )
                    .into())));
                }
            }
        }
    }
}

/// A PostgreSQL `CancelRequest` handle scoped to exactly one `FETCH` command.
pub struct FetchCursorCancelHandle {
    cancel_tx: ShutdownSender,
    cancel_rx: ShutdownToken,
}

impl FetchCursorCancelHandle {
    /// Creates an uncancelled per-`FETCH` handle.
    pub fn new() -> Self {
        let (cancel_tx, cancel_rx) = ShutdownToken::new();
        Self {
            cancel_tx,
            cancel_rx,
        }
    }

    fn register(&self, session: &SessionImpl) {
        session.set_cancel_query_flag(self.cancel_tx.clone());
    }

    async fn cancelled(&mut self) {
        self.cancel_rx.cancelled().await;
    }
}

/// A named SQL cursor managed by one frontend session.
pub enum Cursor {
    /// A cursor that continuously follows a subscription.
    Subscription(SubscriptionCursor),
    /// A cursor over one regular query execution.
    Query(QueryCursor),
}
impl Cursor {
    /// Executes one SQL `FETCH` command against this cursor.
    pub async fn fetch(
        &mut self,
        count: u32,
        handler_args: HandlerArgs,
        formats: &Vec<Format>,
        timeout_seconds: Option<u64>,
        cancel_handle: &mut FetchCursorCancelHandle,
    ) -> Result<(Vec<Row>, Vec<PgFieldDescriptor>)> {
        match self {
            Cursor::Subscription(cursor) => cursor
                .fetch(count, handler_args, formats, timeout_seconds, cancel_handle)
                .await
                .inspect_err(|_| cursor.cursor_metrics.subscription_cursor_error_count.inc()),
            Cursor::Query(cursor) => {
                cursor
                    .fetch(count, formats, handler_args, timeout_seconds, cancel_handle)
                    .await
            }
        }
    }

    /// Returns the fields currently committed by the cursor foreground.
    pub fn get_fields(&mut self) -> Vec<Field> {
        match self {
            Cursor::Subscription(cursor) => cursor.pg_response_stream.fields(),
            Cursor::Query(cursor) => cursor.pg_response_stream.fields(),
        }
    }
}

/// A regular query cursor whose raw producer survives individual `FETCH` cancellation.
pub struct QueryCursor {
    /// Declared first so cursor drop signals shutdown before the producer receiver is dropped.
    lifecycle: CursorLifecycle,
    /// Owns raw-event caching and tentative per-`FETCH` progress.
    pg_response_stream: QueryCursorPgResponseStream,
}

impl QueryCursor {
    /// Creates a lifecycle, schedules a planned query, and starts its raw chunk producer.
    pub(crate) async fn new(
        session: Arc<SessionImpl>,
        plan_fragmenter_result: BatchPlanFragmenterResult,
    ) -> Result<Self> {
        let lifecycle = CursorLifecycle::new(
            session.get_cursor_manager().session_shutdown_token(),
            session
                .env()
                .frontend_config()
                .cursor_data_chunk_channel_capacity,
        );
        let query_shutdown_tx = lifecycle.query_shutdown_sender();
        let query_shutdown_rx = lifecycle.query_shutdown_token();
        let snapshot = session.pinned_snapshot();
        let (query_stream, fields) = crate::handler::declare_cursor::create_cursor_query_stream(
            session,
            plan_fragmenter_result,
            query_shutdown_tx,
            query_shutdown_rx,
            snapshot,
        )
        .await?;
        let chunk_stream =
            QueryCursorDataChunkStream::new(query_stream, fields.clone(), &lifecycle);
        Ok(Self {
            lifecycle,
            pg_response_stream: QueryCursorPgResponseStream::new(chunk_stream, fields),
        })
    }

    /// Executes one SQL `FETCH` command without cancelling the underlying query on timeout or
    /// PostgreSQL `CancelRequest`.
    pub async fn fetch(
        &mut self,
        count: u32,
        formats: &Vec<Format>,
        handler_args: HandlerArgs,
        timeout_seconds: Option<u64>,
        cancel_handle: &mut FetchCursorCancelHandle,
    ) -> Result<(Vec<Row>, Vec<PgFieldDescriptor>)> {
        let session = handler_args.session;
        let mut ans = Vec::with_capacity(std::cmp::min(100, count) as usize);
        let desc = self
            .pg_response_stream
            .fields()
            .iter()
            .map(to_pg_field)
            .collect();
        if count == 0 {
            return Ok((vec![], desc));
        }
        cancel_handle.register(&session);
        self.pg_response_stream.begin_fetch(formats, &session);
        let (mut cursor_shutdown_rx, mut session_shutdown_rx) = self.lifecycle.shutdown_tokens();
        let timeout_instant =
            timeout_seconds.map(|seconds| Instant::now() + Duration::from_secs(seconds));
        let timeout = tokio::time::sleep(Duration::from_secs(timeout_seconds.unwrap_or(0)));
        tokio::pin!(timeout);
        while ans.len() < count as usize {
            tokio::select! {
                biased;
                _ = cancel_handle.cancelled() => {
                    self.pg_response_stream.abort_fetch();
                    return Err(SchedulerError::QueryCancelled(
                        "Cancelled by user".to_owned(),
                    ).into());
                }
                _ = cursor_shutdown_rx.cancelled() => {
                    self.pg_response_stream.abort_fetch();
                    return Err(ErrorCode::InternalError(
                        "Cursor was closed while FETCH was running".to_owned(),
                    ).into());
                }
                _ = session_shutdown_rx.cancelled() => {
                    self.pg_response_stream.abort_fetch();
                    return Err(ErrorCode::InternalError(
                        "Session ended while FETCH was running".to_owned(),
                    ).into());
                }
                row = self.pg_response_stream.next() => match row {
                    Some(Ok(row)) => ans.push(row),
                    Some(Err(error)) => {
                        self.pg_response_stream.abort_fetch();
                        return Err(error);
                    }
                    None => break,
                },
                _ = &mut timeout, if timeout_seconds.is_some() => break,
            }
            if timeout_instant.is_some_and(|timeout_instant| Instant::now() > timeout_instant) {
                break;
            }
        }
        self.pg_response_stream.commit_fetch();
        Ok((ans, desc))
    }
}

#[derive(Clone)]
/// Foreground-visible subscription position received from ordered producer barriers.
enum SubscriptionCursorState {
    /// The producer has not yet started the initial snapshot query for a `FULL` cursor.
    InitSnapshotQuery,
    /// The producer is looking for the next available subscription log-store epoch.
    InitLogStoreQuery {
        /// The timestamp from which the producer will search.
        seek_timestamp: u64,

        /// When present, the next available timestamp must exactly match this value.
        expected_timestamp: Option<u64>,
    },
    /// The producer has started a snapshot or log-store query at this logical position.
    Fetch {
        /// Whether the query reads the initial upstream-table snapshot rather than the log store.
        from_snapshot: bool,

        /// The snapshot epoch or subscription log-store timestamp read by the query.
        rw_timestamp: u64,

        /// The next log-store timestamp expected after this query, when already known.
        expected_timestamp: Option<u64>,

        /// The time at which query initialization began, used only for diagnostics.
        init_query_timer: Instant,
    },
    /// The producer reported an unrecoverable error.
    Invalid,
}

impl Display for SubscriptionCursorState {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            SubscriptionCursorState::InitSnapshotQuery => write!(f, "InitSnapshotQuery"),
            SubscriptionCursorState::InitLogStoreQuery {
                seek_timestamp,
                expected_timestamp,
            } => write!(
                f,
                "InitLogStoreQuery {{ seek_timestamp: {}, expected_timestamp: {:?} }}",
                seek_timestamp, expected_timestamp
            ),
            SubscriptionCursorState::Fetch {
                from_snapshot,
                rw_timestamp,
                expected_timestamp,
                init_query_timer,
                ..
            } => write!(
                f,
                "Fetch {{ from_snapshot: {}, rw_timestamp: {}, expected_timestamp: {:?}, query init at {}ms before }}",
                from_snapshot,
                rw_timestamp,
                expected_timestamp,
                init_query_timer.elapsed().as_millis()
            ),
            SubscriptionCursorState::Invalid => write!(f, "Invalid"),
        }
    }
}

#[derive(Clone)]
/// Maps raw subscription query columns to stable cursor output columns and metadata columns.
struct FieldsManager {
    /// Upstream columns used to detect schema changes between subscription queries.
    columns_catalog: Vec<ColumnCatalog>,
    /// All row fields, including hidden primary keys, operation, timestamp, and visible columns.
    row_fields: Vec<Field>,
    /// Cursor output column indices within [`Self::row_fields`].
    row_output_col_indices: Vec<usize>,
    /// Raw stream-chunk column indices within [`Self::row_fields`].
    stream_chunk_row_indices: Vec<usize>,
    /// Operation-column index within [`Self::row_fields`].
    op_index: usize,
}

impl FieldsManager {
    // pub const OP_FIELD: Field = Field::with_name(DataType::Varchar, "op".to_owned());
    // pub const RW_TIMESTAMP_FIELD: Field = Field::with_name(DataType::Int64, "rw_timestamp".to_owned());

    /// Builds field mappings for the current upstream table catalog.
    pub fn new(catalog: &TableCatalog) -> Self {
        let mut row_fields = Vec::new();
        let mut row_output_col_indices = Vec::new();
        let mut stream_chunk_row_indices = Vec::new();
        let mut output_idx = 0_usize;
        let pk_set: HashSet<usize> = catalog
            .pk
            .iter()
            .map(|col_order| col_order.column_index)
            .collect();

        for (index, v) in catalog.columns.iter().enumerate() {
            if pk_set.contains(&index) {
                stream_chunk_row_indices.push(output_idx);
                row_fields.push(Field::with_name(v.data_type().clone(), v.name()));
                if !v.is_hidden {
                    row_output_col_indices.push(output_idx);
                }
                output_idx += 1;
            } else if !v.is_hidden {
                row_output_col_indices.push(output_idx);
                stream_chunk_row_indices.push(output_idx);
                row_fields.push(Field::with_name(v.data_type().clone(), v.name()));
                output_idx += 1;
            }
        }

        row_fields.push(Field::with_name(DataType::Varchar, "op".to_owned()));
        row_output_col_indices.push(output_idx);
        let op_index = output_idx;
        output_idx += 1;
        row_fields.push(Field::with_name(DataType::Int64, "rw_timestamp".to_owned()));
        row_output_col_indices.push(output_idx);
        Self {
            columns_catalog: catalog.columns.clone(),
            row_fields,
            row_output_col_indices,
            stream_chunk_row_indices,
            op_index,
        }
    }

    /// Rebuilds all mappings when the upstream columns changed.
    pub fn try_refill_fields(&mut self, catalog: &TableCatalog) -> bool {
        if self.columns_catalog.ne(&catalog.columns) {
            *self = Self::new(catalog);
            true
        } else {
            false
        }
    }

    /// Returns fields visible in PostgreSQL cursor responses.
    pub fn get_output_fields(&self) -> Vec<Field> {
        self.row_output_col_indices
            .iter()
            .map(|&idx| self.row_fields[idx].clone())
            .collect()
    }

    /// Maps `FETCH` result formats to raw query fields for snapshot or log-store chunks.
    ///
    /// An empty format list selects PostgreSQL's default text format for every output column.
    pub fn get_row_stream_fields_and_formats(
        &self,
        formats: &[Format],
        from_snapshot: bool,
    ) -> Result<(Vec<Field>, Vec<Format>)> {
        let raw_indices = self
            .stream_chunk_row_indices
            .iter()
            .copied()
            .chain((!from_snapshot).then_some(self.op_index))
            .collect_vec();
        let fields = raw_indices
            .iter()
            .map(|index| self.row_fields[*index].clone())
            .collect();

        if formats.is_empty() {
            return Ok((fields, vec![]));
        }
        let output_formats = FormatIterator::new(formats, self.row_output_col_indices.len())
            .map_err(ErrorCode::InternalError)?;
        let mut row_formats = vec![Format::Text; self.row_fields.len()];
        for (row_index, format) in self
            .row_output_col_indices
            .iter()
            .copied()
            .zip_eq_fast(output_formats)
        {
            row_formats[row_index] = format;
        }
        Ok((fields, row_formats))
    }
}

/// The complete subscription data-fetch state machine, owned only by the producer task.
enum SubscriptionProducerState {
    /// Waits for the first `FETCH` before pinning and querying the `FULL` snapshot.
    InitSnapshotQuery,
    /// Searches for the next available subscription log-store epoch.
    InitLogStoreQuery {
        /// The timestamp from which the producer searches.
        seek_timestamp: u64,
        /// An exact timestamp required to detect retention gaps.
        expected_timestamp: Option<u64>,
    },
    /// Streams one active snapshot or log-store query into the raw-event channel.
    Fetch {
        /// Whether this is the initial upstream-table snapshot.
        from_snapshot: bool,
        /// The snapshot epoch or log-store timestamp read by the query.
        rw_timestamp: u64,
        /// The query stream owned by the producer.
        query_stream: CursorQueryStream,
        /// The next timestamp expected after this query, when known.
        expected_timestamp: Option<u64>,
        /// The query initialization time used by cursor metrics.
        init_query_timer: Instant,
    },
    /// A temporary sentinel used while moving the current state through an async transition.
    Invalid,
}

/// Background subscription state machine that communicates exclusively through raw events.
struct SubscriptionDataChunkProducer {
    subscription: Arc<SubscriptionCatalog>,
    dependent_table_id: TableId,
    handler_args: HandlerArgs,
    fields_manager: FieldsManager,
    state: SubscriptionProducerState,
    cursor_metrics: Arc<CursorMetrics>,
    query_shutdown_tx: ShutdownSender,
    query_shutdown_rx: ShutdownToken,
}

/// A subscription cursor with foreground state committed only from producer channel barriers.
pub struct SubscriptionCursor {
    /// Declared first so cursor drop signals shutdown before the producer receiver is dropped.
    lifecycle: CursorLifecycle,
    cursor_name: String,
    subscription: Arc<SubscriptionCatalog>,
    dependent_table_id: TableId,
    pg_response_stream: SubscriptionCursorPgResponseStream,
    cursor_metrics: Arc<CursorMetrics>,
    last_fetch: Instant,
}

impl SubscriptionCursor {
    /// Declares a deferred subscription cursor without starting its first query.
    pub fn new(
        cursor_name: String,
        start_timestamp: Option<u64>,
        subscription: Arc<SubscriptionCatalog>,
        dependent_table_id: TableId,
        handler_args: &HandlerArgs,
        cursor_metrics: Arc<CursorMetrics>,
    ) -> Result<Self> {
        let lifecycle = CursorLifecycle::new(
            handler_args
                .session
                .get_cursor_manager()
                .session_shutdown_token(),
            handler_args
                .session
                .env()
                .frontend_config()
                .cursor_data_chunk_channel_capacity,
        );
        let query_shutdown_tx = lifecycle.query_shutdown_sender();
        let query_shutdown_rx = lifecycle.query_shutdown_token();
        let table_catalog = handler_args.session.get_table_by_id(dependent_table_id)?;
        let (producer_state, cursor_state) = if let Some(start_timestamp) = start_timestamp {
            (
                SubscriptionProducerState::InitLogStoreQuery {
                    seek_timestamp: start_timestamp,
                    expected_timestamp: None,
                },
                SubscriptionCursorState::InitLogStoreQuery {
                    seek_timestamp: start_timestamp,
                    expected_timestamp: None,
                },
            )
        } else {
            (
                SubscriptionProducerState::InitSnapshotQuery,
                SubscriptionCursorState::InitSnapshotQuery,
            )
        };

        let fields_manager = FieldsManager::new(&table_catalog);
        let output_fields = fields_manager.get_output_fields();
        let expires_at = Instant::now() + Duration::from_secs(subscription.retention_seconds);
        let producer = SubscriptionDataChunkProducer {
            subscription: subscription.clone(),
            dependent_table_id,
            handler_args: handler_args.clone(),
            fields_manager,
            state: producer_state,
            cursor_metrics: cursor_metrics.clone(),
            query_shutdown_tx,
            query_shutdown_rx,
        };
        let chunk_stream = SubscriptionCursorDataChunkStream::new(producer, &lifecycle);

        Ok(Self {
            lifecycle,
            cursor_name,
            subscription,
            dependent_table_id,
            pg_response_stream: SubscriptionCursorPgResponseStream::new(
                chunk_stream,
                output_fields,
                cursor_state,
                expires_at,
            ),
            cursor_metrics,
            last_fetch: Instant::now(),
        })
    }

    /// Executes one SQL `FETCH` while preserving producer progress across timeout or cancellation.
    pub async fn fetch(
        &mut self,
        count: u32,
        handler_args: HandlerArgs,
        formats: &Vec<Format>,
        timeout_seconds: Option<u64>,
        cancel_handle: &mut FetchCursorCancelHandle,
    ) -> Result<(Vec<Row>, Vec<PgFieldDescriptor>)> {
        if self.pg_response_stream.is_expired(Instant::now()) {
            self.lifecycle.shutdown();
            return Err(ErrorCode::InternalError(
                "The cursor has exceeded its maximum lifetime, please recreate it (close then declare cursor).".to_owned(),
            )
            .into());
        }

        let desc = self
            .pg_response_stream
            .fields()
            .iter()
            .map(to_pg_field)
            .collect();
        if count == 0 {
            return Ok((vec![], desc));
        }

        let session = handler_args.session;
        cancel_handle.register(&session);
        let wait_for_data = timeout_seconds.unwrap_or(0) > 0;
        self.pg_response_stream
            .begin_fetch(formats, &session, wait_for_data);
        let (mut cursor_shutdown_rx, mut session_shutdown_rx) = self.lifecycle.shutdown_tokens();
        let timeout_instant =
            timeout_seconds.map(|seconds| Instant::now() + Duration::from_secs(seconds));
        let timeout = tokio::time::sleep(Duration::from_secs(timeout_seconds.unwrap_or(0)));
        tokio::pin!(timeout);
        let fetch_cursor_timer = Instant::now();
        let mut ans = Vec::with_capacity(std::cmp::min(100, count) as usize);
        while ans.len() < count as usize {
            tokio::select! {
                biased;
                _ = cancel_handle.cancelled() => {
                    self.pg_response_stream.abort_fetch();
                    return Err(SchedulerError::QueryCancelled(
                        "Cancelled by user".to_owned(),
                    ).into());
                }
                _ = cursor_shutdown_rx.cancelled() => {
                    self.pg_response_stream.abort_fetch();
                    return Err(ErrorCode::InternalError(
                        "Cursor was closed while FETCH was running".to_owned(),
                    ).into());
                }
                _ = session_shutdown_rx.cancelled() => {
                    self.pg_response_stream.abort_fetch();
                    return Err(ErrorCode::InternalError(
                        "Session ended while FETCH was running".to_owned(),
                    ).into());
                }
                row = self.pg_response_stream.next() => match row {
                    Some(Ok(row)) => ans.push(row),
                    Some(Err(error)) => {
                        self.pg_response_stream.abort_fetch();
                        return Err(error);
                    }
                    None => break,
                },
                _ = &mut timeout, if timeout_seconds.is_some() => break,
            }
            if timeout_instant.is_some_and(|timeout_instant| Instant::now() > timeout_instant) {
                break;
            }
        }
        self.pg_response_stream.commit_fetch();
        self.cursor_metrics
            .subscription_cursor_fetch_duration
            .with_label_values(&[&self.subscription.name])
            .observe(fetch_cursor_timer.elapsed().as_millis() as _);
        self.last_fetch = Instant::now();
        Ok((ans, desc))
    }

    async fn get_next_rw_timestamp(
        seek_timestamp: u64,
        table_id: TableId,
        expected_timestamp: Option<u64>,
        handler_args: HandlerArgs,
        dependent_subscription: &SubscriptionCatalog,
    ) -> Result<(Option<u64>, Option<u64>)> {
        let session = handler_args.session;
        // Test subscription existence
        session.get_subscription_by_schema_id_name(
            dependent_subscription.schema_id,
            &dependent_subscription.name,
        )?;

        // The epoch here must be pulled every time, otherwise there will be cache consistency issues
        let Some(new_epochs) = session
            .list_change_log_epochs(table_id, seek_timestamp, 2)
            .await?
        else {
            return Ok((None, None));
        };
        if let Some(expected_timestamp) = expected_timestamp
            && (new_epochs.is_empty() || &expected_timestamp != new_epochs.first().unwrap())
        {
            return Err(ErrorCode::CatalogError(
                format!(
                    " No data found for rw_timestamp {:?}, data may have been recycled, please recreate cursor",
                    convert_logstore_u64_to_unix_millis(expected_timestamp)
                )
                .into(),
            )
            .into());
        }
        Ok((new_epochs.get(0).cloned(), new_epochs.get(1).cloned()))
    }

    /// Generates a diagnostic batch plan for the foreground-committed subscription position.
    pub fn gen_batch_plan_result(
        &self,
        handler_args: HandlerArgs,
    ) -> Result<RwBatchQueryPlanResult> {
        match self.pg_response_stream.subscription_state().clone() {
            // Only used to return generated plans, so rw_timestamp are meaningless
            SubscriptionCursorState::InitSnapshotQuery => {
                Self::init_batch_plan_for_subscription_cursor(
                    None,
                    self.dependent_table_id,
                    handler_args,
                    None,
                )
            }
            SubscriptionCursorState::InitLogStoreQuery { .. } => {
                Self::init_batch_plan_for_subscription_cursor(
                    Some(0),
                    self.dependent_table_id,
                    handler_args,
                    None,
                )
            }
            SubscriptionCursorState::Fetch {
                from_snapshot,
                rw_timestamp,
                ..
            } => {
                if from_snapshot {
                    Self::init_batch_plan_for_subscription_cursor(
                        None,
                        self.dependent_table_id,
                        handler_args,
                        None,
                    )
                } else {
                    Self::init_batch_plan_for_subscription_cursor(
                        Some(rw_timestamp),
                        self.dependent_table_id,
                        handler_args,
                        None,
                    )
                }
            }
            SubscriptionCursorState::Invalid => Err(ErrorCode::InternalError(
                "Cursor is in invalid state. Please close and re-create the cursor.".to_owned(),
            )
            .into()),
        }
    }

    fn init_batch_plan_for_subscription_cursor(
        rw_timestamp: Option<u64>,
        dependent_table_id: TableId,
        handler_args: HandlerArgs,
        seek_pk_row: Option<Row>,
    ) -> Result<RwBatchQueryPlanResult> {
        let session = handler_args.clone().session;
        let table_catalog = session.get_table_by_id(dependent_table_id)?;
        let context = OptimizerContext::from_handler_args(handler_args);
        let version_id = {
            let version = session.env.hummock_snapshot_manager.acquire();
            let version = version.version();
            if !version
                .state_table_info
                .info()
                .contains_key(&dependent_table_id)
            {
                return Err(anyhow!("table id {dependent_table_id} has been dropped").into());
            }
            version.id
        };
        Self::create_batch_plan_for_cursor(
            table_catalog,
            &session,
            context.into(),
            rw_timestamp.map(|rw_timestamp| (rw_timestamp, rw_timestamp)),
            version_id,
            seek_pk_row,
        )
    }

    async fn initiate_query(
        rw_timestamp: Option<u64>,
        dependent_table_id: TableId,
        handler_args: HandlerArgs,
        query_shutdown_tx: ShutdownSender,
        query_shutdown_rx: ShutdownToken,
        snapshot: ReadSnapshot,
    ) -> Result<(CursorQueryStream, Instant, Arc<TableCatalog>)> {
        let init_query_timer = Instant::now();
        let session = handler_args.clone().session;
        let table_catalog = session.get_table_by_id(dependent_table_id)?;
        let plan_result = Self::init_batch_plan_for_subscription_cursor(
            rw_timestamp,
            dependent_table_id,
            handler_args.clone(),
            None,
        )?;
        let plan_fragmenter_result = gen_batch_plan_fragmenter(&handler_args.session, plan_result)?;
        let (query_stream, _) = crate::handler::declare_cursor::create_cursor_query_stream(
            handler_args.session,
            plan_fragmenter_result,
            query_shutdown_tx,
            query_shutdown_rx,
            snapshot,
        )
        .await?;
        Ok((query_stream, init_query_timer, table_catalog))
    }

    /// Adds subscription operation and timestamp metadata to one encoded PostgreSQL row.
    pub fn build_row(
        mut row: Vec<Option<Bytes>>,
        rw_timestamp: Option<u64>,
        formats: &Vec<Format>,
        session_data: &StaticSessionData,
    ) -> Result<Row> {
        let row_len = row.len();
        let new_row = if let Some(rw_timestamp) = rw_timestamp {
            let rw_timestamp_formats = formats.get(row_len).unwrap_or(&Format::Text);
            let rw_timestamp = convert_logstore_u64_to_unix_millis(rw_timestamp);
            let rw_timestamp = pg_value_format(
                &DataType::Int64,
                risingwave_common::types::ScalarRefImpl::Int64(rw_timestamp as i64),
                *rw_timestamp_formats,
                session_data,
            )?;
            vec![Some(rw_timestamp)]
        } else {
            let op_formats = formats.get(row_len).unwrap_or(&Format::Text);
            let op = pg_value_format(
                &DataType::Varchar,
                risingwave_common::types::ScalarRefImpl::Utf8("Insert"),
                *op_formats,
                session_data,
            )?;
            vec![Some(op), None]
        };
        row.extend(new_row);
        Ok(Row::new(row))
    }

    /// Adds subscription metadata fields to a PostgreSQL row description.
    pub fn build_desc(mut descs: Vec<Field>, from_snapshot: bool) -> Vec<Field> {
        if from_snapshot {
            descs.push(Field::with_name(DataType::Varchar, "op"));
        }
        descs.push(Field::with_name(DataType::Int64, "rw_timestamp"));
        descs
    }

    /// Builds the ordered snapshot or log-store scan plan used by a subscription cursor query.
    pub fn create_batch_plan_for_cursor(
        table_catalog: Arc<TableCatalog>,
        session: &SessionImpl,
        context: OptimizerContextRef,
        epoch_range: Option<(u64, u64)>,
        version_id: HummockVersionId,
        seek_pk_rows: Option<Row>,
    ) -> Result<RwBatchQueryPlanResult> {
        // pk + all column without hidden
        let output_col_idx = table_catalog
            .columns
            .iter()
            .enumerate()
            .filter_map(|(index, v)| {
                if !v.is_hidden || table_catalog.pk.iter().any(|pk| pk.column_index == index) {
                    Some(index)
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        let max_split_range_gap = context.session_ctx().config().max_split_range_gap() as u64;
        let pks = table_catalog.pk();
        let pks = pks
            .iter()
            .map(|f| {
                let pk = table_catalog.columns.get(f.column_index).unwrap();
                (pk.data_type(), f.column_index)
            })
            .collect_vec();
        let (scan, predicate) = if let Some(seek_pk_rows) = seek_pk_rows {
            let mut pk_rows = vec![];
            let mut values = vec![];
            for (seek_pk, (data_type, column_index)) in
                seek_pk_rows.take().into_iter().zip_eq_fast(pks.into_iter())
            {
                if let Some(seek_pk) = seek_pk {
                    pk_rows.push(InputRef {
                        index: column_index,
                        data_type: data_type.clone(),
                    });
                    let value_string = String::from_utf8(seek_pk.clone().into()).unwrap();
                    let value_data = ScalarImpl::from_text(&value_string, data_type).unwrap();
                    values.push((Some(value_data), data_type.clone()));
                }
            }
            if pk_rows.is_empty() {
                (None, None)
            } else {
                let (right_data, right_types): (Vec<_>, Vec<_>) = values.into_iter().unzip();
                let right_data = ScalarImpl::Struct(StructValue::new(right_data));
                let right_type = DataType::Struct(StructType::row_expr_type(right_types));
                let left = FunctionCall::new_unchecked(
                    ExprType::Row,
                    pk_rows.into_iter().map(|pk| pk.into()).collect(),
                    right_type.clone(),
                );
                let right = Literal::new(Some(right_data), right_type);
                let (scan, predicate) = Condition {
                    conjunctions: vec![
                        FunctionCall::new(ExprType::GreaterThan, vec![left.into(), right.into()])?
                            .into(),
                    ],
                }
                .split_to_scan_ranges(&table_catalog, max_split_range_gap)?;
                if scan.len() > 1 {
                    return Err(ErrorCode::InternalError(
                        "Seek pk row should only generate one scan range".to_owned(),
                    )
                    .into());
                }
                (scan.first().cloned(), Some(predicate))
            }
        } else {
            (None, None)
        };

        let (seq_scan, out_fields, out_names) = if let Some(epoch_range) = epoch_range {
            let core = generic::LogScan::new(
                table_catalog.name.clone(),
                output_col_idx,
                table_catalog.clone(),
                context,
                epoch_range,
                version_id,
            );
            let batch_log_seq_scan = BatchLogSeqScan::new(core, scan);
            let out_fields = batch_log_seq_scan.core().out_fields();
            let out_names = batch_log_seq_scan.core().column_names();
            (batch_log_seq_scan.into(), out_fields, out_names)
        } else {
            let core = generic::TableScan::new(
                output_col_idx,
                table_catalog.clone(),
                vec![],
                vec![],
                context,
                Condition {
                    conjunctions: vec![],
                },
                None,
            );
            let scans = match scan {
                Some(scan) => vec![scan],
                None => vec![],
            };
            let table_scan = BatchSeqScan::new(core, scans, None);
            let out_fields = table_scan.core().out_fields();
            let out_names = table_scan.core().column_names();
            (table_scan.into(), out_fields, out_names)
        };

        let plan = if let Some(predicate) = predicate
            && !predicate.always_true()
        {
            BatchFilter::new(generic::Filter::new(predicate, seq_scan)).into()
        } else {
            seq_scan
        };

        // order by pk, so don't need to sort
        let order = Order::new(table_catalog.pk().to_vec());

        // Here we just need a plan_root to call the method, only out_fields and out_names will be used
        let plan_root = PlanRoot::new_with_batch_plan(
            plan,
            RequiredDist::single(),
            order,
            out_fields,
            out_names,
        );
        let schema = plan_root.schema();
        let (batch_log_seq_scan, query_mode) = match session.config().query_mode() {
            QueryMode::Auto | QueryMode::Local => {
                (plan_root.gen_batch_local_plan()?, QueryMode::Local)
            }
            QueryMode::Distributed => (
                plan_root.gen_batch_distributed_plan()?,
                QueryMode::Distributed,
            ),
        };
        Ok(RwBatchQueryPlanResult {
            plan: batch_log_seq_scan,
            query_mode,
            schema,
            stmt_type: StatementType::SELECT,
        })
    }

    /// Returns the duration since the last successful `FETCH`.
    pub fn idle_duration(&self) -> Duration {
        self.last_fetch.elapsed()
    }

    /// Returns the catalog subscription name followed by this cursor.
    pub fn subscription_name(&self) -> &str {
        self.subscription.name.as_str()
    }

    /// Formats the foreground-committed producer position for diagnostics.
    pub fn state_info_string(&self) -> String {
        self.pg_response_stream.subscription_state().to_string()
    }
}

impl SubscriptionDataChunkProducer {
    async fn send_event(
        event_tx: &mpsc::Sender<std::result::Result<CursorDataChunkEvent, BoxedError>>,
        event: std::result::Result<CursorDataChunkEvent, BoxedError>,
        shutdown_rx: &mut ShutdownToken,
        session_shutdown_rx: &mut ShutdownToken,
    ) -> bool {
        tokio::select! {
            biased;
            _ = shutdown_rx.cancelled() => false,
            _ = session_shutdown_rx.cancelled() => false,
            result = event_tx.send(event) => result.is_ok(),
        }
    }

    /// Runs the subscription producer state machine until shutdown or a terminal error.
    async fn run(
        &mut self,
        event_tx: mpsc::Sender<std::result::Result<CursorDataChunkEvent, BoxedError>>,
        shutdown_rx: &mut ShutdownToken,
        session_shutdown_rx: &mut ShutdownToken,
    ) {
        loop {
            let state = mem::replace(&mut self.state, SubscriptionProducerState::Invalid);
            match state {
                SubscriptionProducerState::InitSnapshotQuery => {
                    let snapshot = self.handler_args.session.pinned_snapshot();
                    let pinned_epoch: Result<u64> = match &snapshot {
                        ReadSnapshot::FrontendPinned { snapshot, .. } => snapshot
                            .version()
                            .state_table_info
                            .info()
                            .get(&self.dependent_table_id)
                            .ok_or_else(|| {
                                anyhow!(
                                    "dependent_table_id {} not exists",
                                    self.dependent_table_id
                                )
                                .into()
                            })
                            .map(|info| info.committed_epoch),
                        ReadSnapshot::Other(_) => Err(ErrorCode::InternalError(
                            "Fetch Cursor can't start from specified query epoch. May run `set query_epoch = 0;`"
                                .to_owned(),
                        )
                        .into()),
                        ReadSnapshot::ReadUncommitted => Err(ErrorCode::InternalError(
                            "Fetch Cursor don't support read uncommitted".to_owned(),
                        )
                        .into()),
                    };
                    let pinned_epoch = match pinned_epoch {
                        Ok(pinned_epoch) => pinned_epoch,
                        Err(error) => {
                            _ = Self::send_event(
                                &event_tx,
                                Err(error.into()),
                                shutdown_rx,
                                session_shutdown_rx,
                            )
                            .await;
                            return;
                        }
                    };
                    let initiated = tokio::select! {
                        biased;
                        _ = shutdown_rx.cancelled() => return,
                        _ = session_shutdown_rx.cancelled() => return,
                        result = SubscriptionCursor::initiate_query(
                            None,
                            self.dependent_table_id,
                            self.handler_args.clone(),
                            self.query_shutdown_tx.clone(),
                            self.query_shutdown_rx.clone(),
                            snapshot,
                        ) => result,
                    };
                    let (query_stream, init_query_timer, catalog) = match initiated {
                        Ok(result) => result,
                        Err(error) => {
                            _ = Self::send_event(
                                &event_tx,
                                Err(error.into()),
                                shutdown_rx,
                                session_shutdown_rx,
                            )
                            .await;
                            return;
                        }
                    };
                    let schema_changed = self.fields_manager.try_refill_fields(&catalog);
                    let cursor_state = SubscriptionCursorState::Fetch {
                        from_snapshot: true,
                        rw_timestamp: pinned_epoch,
                        expected_timestamp: None,
                        init_query_timer,
                    };
                    self.state = SubscriptionProducerState::Fetch {
                        from_snapshot: true,
                        rw_timestamp: pinned_epoch,
                        query_stream,
                        expected_timestamp: None,
                        init_query_timer,
                    };
                    if !Self::send_event(
                        &event_tx,
                        Ok(CursorDataChunkEvent::Barrier(
                            CursorDataChunkBarrier::SubscriptionQueryStarted {
                                state: cursor_state,
                                output_fields: self.fields_manager.get_output_fields(),
                                expires_at: Instant::now()
                                    + Duration::from_secs(self.subscription.retention_seconds),
                            },
                        )),
                        shutdown_rx,
                        session_shutdown_rx,
                    )
                    .await
                    {
                        return;
                    }
                    if schema_changed
                        && !Self::send_event(
                            &event_tx,
                            Ok(CursorDataChunkEvent::Barrier(
                                CursorDataChunkBarrier::SchemaChanged,
                            )),
                            shutdown_rx,
                            session_shutdown_rx,
                        )
                        .await
                    {
                        return;
                    }
                }
                SubscriptionProducerState::InitLogStoreQuery {
                    seek_timestamp,
                    expected_timestamp,
                } => {
                    let next_timestamp = tokio::select! {
                        biased;
                        _ = shutdown_rx.cancelled() => return,
                        _ = session_shutdown_rx.cancelled() => return,
                        result = SubscriptionCursor::get_next_rw_timestamp(
                            seek_timestamp,
                            self.dependent_table_id,
                            expected_timestamp,
                            self.handler_args.clone(),
                            &self.subscription,
                        ) => result,
                    };
                    let (rw_timestamp, next_expected_timestamp) = match next_timestamp {
                        Ok((Some(rw_timestamp), next_expected_timestamp)) => {
                            (rw_timestamp, next_expected_timestamp)
                        }
                        Ok((None, _)) => {
                            self.state = SubscriptionProducerState::InitLogStoreQuery {
                                seek_timestamp,
                                expected_timestamp,
                            };
                            if !Self::send_event(
                                &event_tx,
                                Ok(CursorDataChunkEvent::Barrier(
                                    CursorDataChunkBarrier::SubscriptionIdle,
                                )),
                                shutdown_rx,
                                session_shutdown_rx,
                            )
                            .await
                            {
                                return;
                            }
                            let notification = tokio::select! {
                                biased;
                                _ = shutdown_rx.cancelled() => return,
                                _ = session_shutdown_rx.cancelled() => return,
                                result = self.handler_args.session.env
                                    .hummock_snapshot_manager()
                                    .wait_table_change_log_notification(
                                        self.dependent_table_id,
                                        seek_timestamp,
                                    ) => result,
                            };
                            if let Err(error) = notification {
                                _ = Self::send_event(
                                    &event_tx,
                                    Err(error.into()),
                                    shutdown_rx,
                                    session_shutdown_rx,
                                )
                                .await;
                                return;
                            }
                            continue;
                        }
                        Err(error) => {
                            _ = Self::send_event(
                                &event_tx,
                                Err(error.into()),
                                shutdown_rx,
                                session_shutdown_rx,
                            )
                            .await;
                            return;
                        }
                    };

                    // The cursor producer can outlive the FETCH transaction, so its query must
                    // own a snapshot instead of consulting the session transaction state.
                    let snapshot = ReadSnapshot::FrontendPinned {
                        snapshot: self
                            .handler_args
                            .session
                            .env
                            .hummock_snapshot_manager()
                            .acquire(),
                    };
                    let initiated = tokio::select! {
                        biased;
                        _ = shutdown_rx.cancelled() => return,
                        _ = session_shutdown_rx.cancelled() => return,
                        result = SubscriptionCursor::initiate_query(
                            Some(rw_timestamp),
                            self.dependent_table_id,
                            self.handler_args.clone(),
                            self.query_shutdown_tx.clone(),
                            self.query_shutdown_rx.clone(),
                            snapshot,
                        ) => result,
                    };
                    let (query_stream, init_query_timer, catalog) = match initiated {
                        Ok(result) => result,
                        Err(error) => {
                            _ = Self::send_event(
                                &event_tx,
                                Err(error.into()),
                                shutdown_rx,
                                session_shutdown_rx,
                            )
                            .await;
                            return;
                        }
                    };
                    let schema_changed = self.fields_manager.try_refill_fields(&catalog);
                    let cursor_state = SubscriptionCursorState::Fetch {
                        from_snapshot: false,
                        rw_timestamp,
                        expected_timestamp: next_expected_timestamp,
                        init_query_timer,
                    };
                    self.state = SubscriptionProducerState::Fetch {
                        from_snapshot: false,
                        rw_timestamp,
                        query_stream,
                        expected_timestamp: next_expected_timestamp,
                        init_query_timer,
                    };
                    if !Self::send_event(
                        &event_tx,
                        Ok(CursorDataChunkEvent::Barrier(
                            CursorDataChunkBarrier::SubscriptionQueryStarted {
                                state: cursor_state,
                                output_fields: self.fields_manager.get_output_fields(),
                                expires_at: Instant::now()
                                    + Duration::from_secs(self.subscription.retention_seconds),
                            },
                        )),
                        shutdown_rx,
                        session_shutdown_rx,
                    )
                    .await
                    {
                        return;
                    }
                    if schema_changed
                        && !Self::send_event(
                            &event_tx,
                            Ok(CursorDataChunkEvent::Barrier(
                                CursorDataChunkBarrier::SchemaChanged,
                            )),
                            shutdown_rx,
                            session_shutdown_rx,
                        )
                        .await
                    {
                        return;
                    }
                }
                SubscriptionProducerState::Fetch {
                    from_snapshot,
                    rw_timestamp,
                    mut query_stream,
                    expected_timestamp,
                    init_query_timer,
                } => {
                    let next = tokio::select! {
                        biased;
                        _ = shutdown_rx.cancelled() => return,
                        _ = session_shutdown_rx.cancelled() => return,
                        next = query_stream.next() => next,
                    };
                    match next {
                        Some(Ok(chunk)) => {
                            self.state = SubscriptionProducerState::Fetch {
                                from_snapshot,
                                rw_timestamp,
                                query_stream,
                                expected_timestamp,
                                init_query_timer,
                            };
                            let data = CursorDataChunk {
                                chunk,
                                kind: CursorDataChunkKind::Subscription {
                                    fields: Arc::new(self.fields_manager.clone()),
                                    from_snapshot,
                                    rw_timestamp,
                                },
                            };
                            if !Self::send_event(
                                &event_tx,
                                Ok(CursorDataChunkEvent::Chunk(data)),
                                shutdown_rx,
                                session_shutdown_rx,
                            )
                            .await
                            {
                                return;
                            }
                        }
                        Some(Err(error)) => {
                            _ = Self::send_event(
                                &event_tx,
                                Err(error),
                                shutdown_rx,
                                session_shutdown_rx,
                            )
                            .await;
                            return;
                        }
                        None => {
                            self.cursor_metrics
                                .subscription_cursor_query_duration
                                .with_label_values(&[&self.subscription.name])
                                .observe(init_query_timer.elapsed().as_millis() as _);
                            let (seek_timestamp, expected_timestamp) =
                                if let Some(expected_timestamp) = expected_timestamp {
                                    (expected_timestamp, Some(expected_timestamp))
                                } else {
                                    (rw_timestamp + 1, None)
                                };
                            self.state = SubscriptionProducerState::InitLogStoreQuery {
                                seek_timestamp,
                                expected_timestamp,
                            };
                            let next_state = SubscriptionCursorState::InitLogStoreQuery {
                                seek_timestamp,
                                expected_timestamp,
                            };
                            if !Self::send_event(
                                &event_tx,
                                Ok(CursorDataChunkEvent::Barrier(
                                    CursorDataChunkBarrier::SubscriptionBatch { next_state },
                                )),
                                shutdown_rx,
                                session_shutdown_rx,
                            )
                            .await
                            {
                                return;
                            }
                        }
                    }
                }
                SubscriptionProducerState::Invalid => return,
            }
        }
    }
}

/// Owns every named cursor and the session-wide cursor shutdown signal.
pub struct CursorManager {
    cursor_map: tokio::sync::Mutex<HashMap<String, Cursor>>,
    cursor_metrics: Arc<CursorMetrics>,
    session_shutdown_tx: ShutdownSender,
    session_shutdown_rx: ShutdownToken,
}

impl CursorManager {
    /// Creates an empty cursor manager for one frontend session.
    pub fn new(cursor_metrics: Arc<CursorMetrics>) -> Self {
        let (session_shutdown_tx, session_shutdown_rx) = ShutdownToken::new();
        Self {
            cursor_map: tokio::sync::Mutex::new(HashMap::new()),
            cursor_metrics,
            session_shutdown_tx,
            session_shutdown_rx,
        }
    }

    /// Returns a token observed by every producer and foreground stream in this session.
    pub fn session_shutdown_token(&self) -> ShutdownToken {
        self.session_shutdown_rx.clone()
    }

    /// Signals all cursor foregrounds, producers, and query executors to stop cooperatively.
    pub fn shutdown(&self) {
        self.session_shutdown_tx.cancel();
    }

    /// Declares and registers a subscription cursor.
    pub async fn add_subscription_cursor(
        &self,
        cursor_name: String,
        start_timestamp: Option<u64>,
        dependent_table_id: TableId,
        subscription: Arc<SubscriptionCatalog>,
        handler_args: &HandlerArgs,
    ) -> Result<()> {
        let create_cursor_timer = Instant::now();
        let subscription_name = subscription.name.clone();
        let cursor = SubscriptionCursor::new(
            cursor_name,
            start_timestamp,
            subscription,
            dependent_table_id,
            handler_args,
            self.cursor_metrics.clone(),
        )?;
        let mut cursor_map = self.cursor_map.lock().await;
        self.cursor_metrics
            .subscription_cursor_declare_duration
            .with_label_values(&[&subscription_name])
            .observe(create_cursor_timer.elapsed().as_millis() as _);

        cursor_map.retain(|_, v| {
            if let Cursor::Subscription(cursor) = v
                && cursor.pg_response_stream.is_failed()
            {
                false
            } else {
                true
            }
        });

        cursor_map
            .try_insert(cursor.cursor_name.clone(), Cursor::Subscription(cursor))
            .map_err(|error| {
                ErrorCode::CatalogError(
                    format!("cursor `{}` already exists", error.entry.key()).into(),
                )
            })?;
        Ok(())
    }

    /// Registers a regular query cursor and its stable output fields.
    pub async fn add_query_cursor(&self, cursor_name: String, cursor: QueryCursor) -> Result<()> {
        self.cursor_map
            .lock()
            .await
            .try_insert(cursor_name, Cursor::Query(cursor))
            .map_err(|error| {
                ErrorCode::CatalogError(
                    format!("cursor `{}` already exists", error.entry.key()).into(),
                )
            })?;

        Ok(())
    }

    /// Closes and removes one named cursor.
    pub async fn remove_cursor(&self, cursor_name: &str) -> Result<()> {
        self.cursor_map
            .lock()
            .await
            .remove(cursor_name)
            .ok_or_else(|| {
                ErrorCode::CatalogError(format!("cursor `{}` don't exists", cursor_name).into())
            })?;
        Ok(())
    }

    /// Closes and removes every cursor in the session.
    pub async fn remove_all_cursor(&self) {
        self.cursor_map.lock().await.clear();
    }

    /// Closes regular query cursors while retaining subscription cursors.
    pub async fn remove_all_query_cursor(&self) {
        self.cursor_map
            .lock()
            .await
            .retain(|_, v| matches!(v, Cursor::Subscription(_)));
    }

    /// Executes one `FETCH` against a named cursor.
    pub async fn get_rows_with_cursor(
        &self,
        cursor_name: &str,
        count: u32,
        handler_args: HandlerArgs,
        formats: &Vec<Format>,
        timeout_seconds: Option<u64>,
        cancel_handle: &mut FetchCursorCancelHandle,
    ) -> Result<(Vec<Row>, Vec<PgFieldDescriptor>)> {
        if let Some(cursor) = self.cursor_map.lock().await.get_mut(cursor_name) {
            cursor
                .fetch(count, handler_args, formats, timeout_seconds, cancel_handle)
                .await
        } else {
            Err(ErrorCode::InternalError(format!("Cannot find cursor `{}`", cursor_name)).into())
        }
    }

    /// Returns the foreground-committed fields of a named cursor.
    pub async fn get_fields_with_cursor(&self, cursor_name: &str) -> Result<Vec<Field>> {
        if let Some(cursor) = self.cursor_map.lock().await.get_mut(cursor_name) {
            Ok(cursor.get_fields())
        } else {
            Err(ErrorCode::InternalError(format!("Cannot find cursor `{}`", cursor_name)).into())
        }
    }

    /// Collects periodic cursor counts and last-fetch durations.
    pub async fn get_periodic_cursor_metrics(&self) -> PeriodicCursorMetrics {
        let mut subscription_cursor_nums = 0;
        let mut invalid_subscription_cursor_nums = 0;
        let mut subscription_cursor_last_fetch_duration = HashMap::new();
        for cursor in self.cursor_map.lock().await.values() {
            if let Cursor::Subscription(subscription_cursor) = cursor {
                subscription_cursor_nums += 1;
                if subscription_cursor.pg_response_stream.is_failed() {
                    invalid_subscription_cursor_nums += 1;
                } else {
                    let fetch_duration =
                        subscription_cursor.last_fetch.elapsed().as_millis() as f64;
                    subscription_cursor_last_fetch_duration.insert(
                        subscription_cursor.subscription.name.clone(),
                        fetch_duration,
                    );
                }
            }
        }
        PeriodicCursorMetrics {
            subscription_cursor_nums,
            invalid_subscription_cursor_nums,
            subscription_cursor_last_fetch_duration,
        }
    }

    /// Applies a callback to every regular query cursor while holding the manager lock.
    pub async fn iter_query_cursors(&self, mut f: impl FnMut(&String, &QueryCursor)) {
        self.cursor_map
            .lock()
            .await
            .iter()
            .for_each(|(cursor_name, cursor)| {
                if let Cursor::Query(cursor) = cursor {
                    f(cursor_name, cursor)
                }
            });
    }

    /// Applies a callback to every subscription cursor while holding the manager lock.
    pub async fn iter_subscription_cursors(&self, mut f: impl FnMut(&String, &SubscriptionCursor)) {
        self.cursor_map
            .lock()
            .await
            .iter()
            .for_each(|(cursor_name, cursor)| {
                if let Cursor::Subscription(cursor) = cursor {
                    f(cursor_name, cursor)
                }
            });
    }

    /// Generates the batch plan corresponding to a subscription cursor's committed position.
    pub async fn gen_batch_plan_with_subscription_cursor(
        &self,
        cursor_name: &str,
        handler_args: HandlerArgs,
    ) -> Result<RwBatchQueryPlanResult> {
        match self.cursor_map.lock().await.get(cursor_name).ok_or_else(|| {
            ErrorCode::InternalError(format!("Cannot find cursor `{}`", cursor_name))
        })? {
            Cursor::Subscription(cursor) => {
                cursor.gen_batch_plan_result(handler_args.clone())
            },
            Cursor::Query(_) => Err(ErrorCode::InternalError("The plan of the cursor is the same as the query statement of the as when it was created.".to_owned()).into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};

    use risingwave_common::array::DataChunkTestExt;
    use risingwave_common::config::FrontendConfig;
    use risingwave_sqlparser::parser::Parser;

    use super::*;

    impl CursorDataChunkReceiver {
        /// Creates a synthetic producer stream with an explicitly configured channel capacity.
        fn for_test(
            events: Vec<CursorDataChunkEvent>,
            lifecycle: &CursorLifecycle,
            start_rx: Option<oneshot::Receiver<()>>,
        ) -> (Self, Arc<AtomicBool>) {
            let (event_tx, event_rx) = mpsc::channel(lifecycle.data_chunk_channel_capacity);
            let mut producer_shutdown_rx = lifecycle.shutdown_rx.clone();
            let started = Arc::new(AtomicBool::new(false));
            let started_for_task = started.clone();
            let producer_handle = tokio::spawn(async move {
                if let Some(start_rx) = start_rx {
                    tokio::select! {
                        biased;
                        _ = producer_shutdown_rx.cancelled() => return,
                        _ = start_rx => {}
                    }
                }
                started_for_task.store(true, Ordering::Relaxed);
                for event in events {
                    if event_tx.send(Ok(event)).await.is_err() {
                        return;
                    }
                }
                producer_shutdown_rx.cancelled().await;
            });
            (
                Self {
                    event_rx,
                    producer_handle: Some(producer_handle),
                },
                started,
            )
        }
    }

    impl QueryCursorDataChunkStream {
        /// Creates an immediately started synthetic query producer.
        fn for_test(
            events: Vec<CursorDataChunkEvent>,
            lifecycle: &CursorLifecycle,
        ) -> (Self, Arc<AtomicBool>) {
            let (receiver, started) = CursorDataChunkReceiver::for_test(events, lifecycle, None);
            (Self { receiver }, started)
        }
    }

    impl SubscriptionCursorDataChunkStream {
        /// Creates a deferred synthetic subscription producer.
        fn for_test(
            events: Vec<CursorDataChunkEvent>,
            lifecycle: &CursorLifecycle,
        ) -> (Self, Arc<AtomicBool>) {
            let (start_tx, start_rx) = oneshot::channel();
            let (receiver, started) =
                CursorDataChunkReceiver::for_test(events, lifecycle, Some(start_rx));
            (
                Self {
                    receiver,
                    start_tx: Some(start_tx),
                },
                started,
            )
        }

        /// Returns the configured event-channel capacity.
        fn channel_capacity(&self) -> usize {
            self.receiver.event_rx.max_capacity()
        }
    }

    fn handler_args(session: Arc<SessionImpl>) -> HandlerArgs {
        let sql: Arc<str> = "select 1".into();
        let statement = Parser::parse_exactly_one(&sql).unwrap();
        HandlerArgs::new(session, &statement, sql).unwrap()
    }

    fn test_lifecycle(channel_capacity: usize) -> CursorLifecycle {
        let (_, session_shutdown_rx) = ShutdownToken::new();
        CursorLifecycle::new(session_shutdown_rx, channel_capacity)
    }

    fn query_data_stream(
        channel_capacity: usize,
    ) -> (
        CursorLifecycle,
        QueryCursorDataChunkStream,
        Vec<Field>,
        Arc<AtomicBool>,
    ) {
        let fields = vec![Field::with_name(DataType::Int32, "v")];
        let events = vec![
            CursorDataChunkEvent::Chunk(CursorDataChunk {
                chunk: DataChunk::from_pretty(
                    "i
                     1
                     2
                     3",
                ),
                kind: CursorDataChunkKind::Query {
                    fields: Arc::new(fields.clone()),
                },
            }),
            CursorDataChunkEvent::Barrier(CursorDataChunkBarrier::QueryEnd),
        ];
        let lifecycle = test_lifecycle(channel_capacity);
        let (stream, started) = QueryCursorDataChunkStream::for_test(events, &lifecycle);
        (lifecycle, stream, fields, started)
    }

    fn subscription_fields() -> FieldsManager {
        FieldsManager {
            columns_catalog: vec![],
            row_fields: vec![
                Field::with_name(DataType::Int32, "v"),
                Field::with_name(DataType::Varchar, "op"),
                Field::with_name(DataType::Int64, "rw_timestamp"),
            ],
            row_output_col_indices: vec![0, 1, 2],
            stream_chunk_row_indices: vec![0],
            op_index: 1,
        }
    }

    fn subscription_data_stream(
        from_snapshot: bool,
        channel_capacity: usize,
    ) -> (
        CursorLifecycle,
        SubscriptionCursorDataChunkStream,
        Arc<AtomicBool>,
    ) {
        let fields = Arc::new(subscription_fields());
        let fetch_state = SubscriptionCursorState::Fetch {
            from_snapshot,
            rw_timestamp: 1 << 16,
            expected_timestamp: None,
            init_query_timer: Instant::now(),
        };
        let chunk = if from_snapshot {
            DataChunk::from_pretty(
                "i
                 1
                 2
                 3",
            )
        } else {
            DataChunk::from_pretty(
                "i T
                 1 Insert
                 2 Insert
                 3 Insert",
            )
        };
        let events = vec![
            CursorDataChunkEvent::Barrier(CursorDataChunkBarrier::SubscriptionQueryStarted {
                state: fetch_state,
                output_fields: fields.get_output_fields(),
                expires_at: Instant::now() + Duration::from_secs(60),
            }),
            CursorDataChunkEvent::Chunk(CursorDataChunk {
                chunk,
                kind: CursorDataChunkKind::Subscription {
                    fields: fields.clone(),
                    from_snapshot,
                    rw_timestamp: 1 << 16,
                },
            }),
            CursorDataChunkEvent::Barrier(CursorDataChunkBarrier::SubscriptionBatch {
                next_state: SubscriptionCursorState::InitLogStoreQuery {
                    seek_timestamp: (1 << 16) + 1,
                    expected_timestamp: None,
                },
            }),
        ];
        let lifecycle = test_lifecycle(channel_capacity);
        let (stream, started) = SubscriptionCursorDataChunkStream::for_test(events, &lifecycle);
        (lifecycle, stream, started)
    }

    fn subscription_cursor(
        lifecycle: CursorLifecycle,
        chunk_stream: SubscriptionCursorDataChunkStream,
        from_snapshot: bool,
    ) -> SubscriptionCursor {
        let state = if from_snapshot {
            SubscriptionCursorState::InitSnapshotQuery
        } else {
            SubscriptionCursorState::InitLogStoreQuery {
                seek_timestamp: 1 << 16,
                expected_timestamp: None,
            }
        };
        SubscriptionCursor {
            lifecycle,
            cursor_name: "cur".to_owned(),
            subscription: Arc::new(SubscriptionCatalog {
                name: "sub".to_owned(),
                retention_seconds: 60,
                ..Default::default()
            }),
            dependent_table_id: 0.into(),
            pg_response_stream: SubscriptionCursorPgResponseStream::new(
                chunk_stream,
                subscription_fields().get_output_fields(),
                state,
                Instant::now() + Duration::from_secs(60),
            ),
            cursor_metrics: Arc::new(CursorMetrics::for_test()),
            last_fetch: Instant::now(),
        }
    }

    fn assert_text_value(row: &Row, expected: &[u8]) {
        assert_eq!(row.values()[0].as_ref().unwrap().as_ref(), expected);
    }

    fn assert_binary_i32(row: &Row, expected: i32) {
        assert_eq!(
            row.values()[0].as_ref().unwrap().as_ref(),
            expected.to_be_bytes()
        );
    }

    /// Verifies rollback-safe caching for a regular query response stream.
    async fn assert_query_cancelled_fetch_keeps_response_stream_cache(
        mut response_stream: QueryCursorPgResponseStream,
        session: &SessionImpl,
        formats: Vec<Format>,
    ) {
        response_stream.begin_fetch(&[], session);
        let row = response_stream.next().await.unwrap().unwrap();
        assert_text_value(&row, b"1");
        // CancelRequest discards the tentative read position, while the raw chunk remains in
        // this cursor-owned response stream.
        response_stream.abort_fetch();
        assert!(!response_stream.common.cached_events.is_empty());

        response_stream.begin_fetch(&formats, session);
        let row = response_stream.next().await.unwrap().unwrap();
        assert_binary_i32(&row, 1);
        response_stream.commit_fetch();
    }

    /// Verifies rollback-safe caching and metadata for a subscription response stream.
    async fn assert_subscription_cancelled_fetch_keeps_response_stream_cache(
        mut response_stream: SubscriptionCursorPgResponseStream,
        session: &SessionImpl,
        formats: Vec<Format>,
    ) {
        response_stream.begin_fetch(&[], session, false);
        let row = response_stream.next().await.unwrap().unwrap();
        assert_text_value(&row, b"1");
        response_stream.abort_fetch();
        assert!(!response_stream.common.cached_events.is_empty());
        assert!(matches!(
            response_stream.subscription_state(),
            SubscriptionCursorState::InitSnapshotQuery
        ));

        response_stream.begin_fetch(&formats, session, false);
        let row = response_stream.next().await.unwrap().unwrap();
        assert_binary_i32(&row, 1);
        response_stream.commit_fetch();
        assert!(matches!(
            response_stream.subscription_state(),
            SubscriptionCursorState::Fetch {
                from_snapshot: true,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn test_dropping_unfinished_local_cursor_query_cancels_executor() {
        let (shutdown_tx, shutdown_rx) = ShutdownToken::new();
        let (_chunk_tx, chunk_rx) = mpsc::channel(1);
        let query_stream = CursorQueryStream::local(
            tokio_stream::wrappers::ReceiverStream::new(chunk_rx),
            shutdown_tx,
        );

        drop(query_stream);

        assert!(shutdown_rx.is_cancelled());
    }

    #[tokio::test]
    async fn test_query_cancel_timeout_and_format_changes_preserve_cursor_data() {
        let session = Arc::new(SessionImpl::mock());
        let args = handler_args(session.clone());
        let capacity = session
            .env()
            .frontend_config()
            .cursor_data_chunk_channel_capacity;
        let (lifecycle, chunk_stream, fields, _) = query_data_stream(capacity);
        let mut cursor = QueryCursor {
            lifecycle,
            pg_response_stream: QueryCursorPgResponseStream::new(chunk_stream, fields),
        };

        let mut cancelled_fetch = FetchCursorCancelHandle::new();
        assert!(cancelled_fetch.cancel_tx.cancel());
        let error = cursor
            .fetch(3, &vec![], args.clone(), None, &mut cancelled_fetch)
            .await
            .expect_err("CancelRequest must terminate only this FETCH");
        assert!(error.to_string().contains("Cancelled by user"));
        session.clear_cancel_query_flag();

        let (rows, _) = cursor
            .fetch(
                3,
                &vec![],
                args.clone(),
                Some(0),
                &mut FetchCursorCancelHandle::new(),
            )
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_text_value(&rows[0], b"1");

        let (rows, _) = cursor
            .fetch(
                3,
                &vec![Format::Binary],
                args,
                None,
                &mut FetchCursorCancelHandle::new(),
            )
            .await
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert_binary_i32(&rows[0], 2);
        assert_binary_i32(&rows[1], 3);
    }

    #[tokio::test]
    async fn test_subscription_full_and_since_fetch_lifecycles() {
        for from_snapshot in [true, false] {
            let session = Arc::new(SessionImpl::mock());
            let args = handler_args(session.clone());
            let capacity = session
                .env()
                .frontend_config()
                .cursor_data_chunk_channel_capacity;
            let (lifecycle, chunk_stream, started) =
                subscription_data_stream(from_snapshot, capacity);
            let mut cursor = subscription_cursor(lifecycle, chunk_stream, from_snapshot);

            tokio::task::yield_now().await;
            assert!(
                !started.load(Ordering::Relaxed),
                "FULL and SINCE both defer production until their first FETCH"
            );

            let mut cancelled_fetch = FetchCursorCancelHandle::new();
            assert!(cancelled_fetch.cancel_tx.cancel());
            let error = cursor
                .fetch(3, args.clone(), &vec![], None, &mut cancelled_fetch)
                .await
                .expect_err("CancelRequest must terminate only this FETCH");
            assert!(error.to_string().contains("Cancelled by user"));
            session.clear_cancel_query_flag();
            tokio::task::yield_now().await;
            assert!(started.load(Ordering::Relaxed));

            let (rows, _) = cursor
                .fetch(
                    3,
                    args.clone(),
                    &vec![],
                    Some(0),
                    &mut FetchCursorCancelHandle::new(),
                )
                .await
                .unwrap();
            assert_eq!(rows.len(), 1);
            assert_text_value(&rows[0], b"1");

            let (rows, _) = cursor
                .fetch(
                    3,
                    args,
                    &[Format::Binary, Format::Text, Format::Binary].to_vec(),
                    None,
                    &mut FetchCursorCancelHandle::new(),
                )
                .await
                .unwrap();
            assert_eq!(rows.len(), 2);
            assert_binary_i32(&rows[0], 2);
            assert_binary_i32(&rows[1], 3);
        }
    }

    #[tokio::test]
    async fn test_cancelled_fetch_keeps_response_stream_cache() {
        let session = SessionImpl::mock();
        let capacity = session
            .env()
            .frontend_config()
            .cursor_data_chunk_channel_capacity;

        let (_query_lifecycle, stream, fields, _) = query_data_stream(capacity);
        assert_query_cancelled_fetch_keeps_response_stream_cache(
            QueryCursorPgResponseStream::new(stream, fields),
            &session,
            vec![Format::Binary],
        )
        .await;

        let (_subscription_lifecycle, stream, _) = subscription_data_stream(true, capacity);
        assert_subscription_cancelled_fetch_keeps_response_stream_cache(
            SubscriptionCursorPgResponseStream::new(
                stream,
                subscription_fields().get_output_fields(),
                SubscriptionCursorState::InitSnapshotQuery,
                Instant::now() + Duration::from_secs(60),
            ),
            &session,
            vec![Format::Binary, Format::Text, Format::Binary],
        )
        .await;
    }

    #[tokio::test]
    async fn test_cursor_test_stream_uses_configured_channel_capacity() {
        let config: FrontendConfig =
            toml::from_str("cursor_data_chunk_channel_capacity = 3").unwrap();
        let (_lifecycle, stream, _) =
            subscription_data_stream(true, config.cursor_data_chunk_channel_capacity);
        assert_eq!(stream.channel_capacity(), 3);
    }

    #[tokio::test]
    async fn test_subscription_idle_barrier_remains_nonblocking_across_fetches() {
        let lifecycle = test_lifecycle(1);
        let (stream, _) = SubscriptionCursorDataChunkStream::for_test(
            vec![CursorDataChunkEvent::Barrier(
                CursorDataChunkBarrier::SubscriptionIdle,
            )],
            &lifecycle,
        );
        let mut response_stream = SubscriptionCursorPgResponseStream::new(
            stream,
            subscription_fields().get_output_fields(),
            SubscriptionCursorState::InitLogStoreQuery {
                seek_timestamp: 1 << 16,
                expected_timestamp: None,
            },
            Instant::now() + Duration::from_secs(60),
        );
        let session = SessionImpl::mock();

        for _ in 0..2 {
            response_stream.begin_fetch(&[], &session, false);
            assert!(
                tokio::time::timeout(Duration::from_secs(1), response_stream.next())
                    .await
                    .expect("non-waiting FETCH must not block on a committed idle barrier")
                    .is_none()
            );
            response_stream.commit_fetch();
        }
    }
}
