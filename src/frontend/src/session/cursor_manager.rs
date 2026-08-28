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
use std::sync::{Arc, Weak};
use std::task::{Context, Poll};
use std::time::Instant;

use anyhow::anyhow;
use bytes::Bytes;
use futures::stream::BoxStream;
use futures::{Stream, StreamExt};
use futures_async_stream::try_stream;
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
use crate::utils::{Condition, WithOptions};
use crate::{OptimizerContext, OptimizerContextRef, TableCatalog};

/// Cancellation resources shared by a cursor and its underlying query executors.
struct CursorLifecycle {
    /// Sender used to stop every query executor owned by this cursor.
    shutdown_tx: ShutdownSender,
    /// Cloneable token through which the cursor and its query executors observe cursor shutdown.
    shutdown_rx: ShutdownToken,
    /// Cloneable token through which this cursor observes termination of its frontend session.
    session_shutdown_rx: ShutdownToken,
}

impl CursorLifecycle {
    /// Creates lifecycle resources for a cursor in the given session.
    fn new(session_shutdown_rx: ShutdownToken) -> Self {
        let (shutdown_tx, shutdown_rx) = ShutdownToken::new();
        Self {
            shutdown_tx,
            shutdown_rx,
            session_shutdown_rx,
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

    /// Signals this cursor's underlying query executor to stop cooperatively.
    fn shutdown(&self) {
        self.shutdown_tx.cancel();
    }
}

impl Drop for CursorLifecycle {
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// The local or distributed query stream owned by a cursor.
enum CursorQueryStreamInner {
    /// A query executed by the frontend's local batch executor.
    Local {
        /// Data chunks produced by the local batch executor.
        stream: LocalQueryStream,
        /// Sender used to cancel the local executor if this stream is dropped before EOF.
        shutdown_tx: ShutdownSender,
    },
    /// A query scheduled through the distributed query manager.
    Distributed {
        /// Data chunks produced by the distributed query execution.
        stream: DistributedQueryStream,
        /// Manager used to cancel the distributed query if this stream is dropped before EOF.
        query_manager: QueryManager,
        /// Identifier passed to [`QueryManager`] when cancelling the distributed query.
        query_id: QueryId,
    },
}

/// A cursor-owned query stream that cancels unfinished execution when dropped.
pub struct CursorQueryStream {
    /// Concrete local or distributed query stream and its cancellation resources.
    inner: CursorQueryStreamInner,
    /// Whether the concrete stream has reached EOF and therefore no longer needs cancellation.
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
    Query {
        /// Fields used to format every raw column in the query chunk.
        fields: Arc<Vec<Field>>,
    },
    /// A chunk from either the snapshot or log-store phase of a subscription cursor.
    Subscription {
        /// Field mapping that was current when this chunk was produced.
        fields: Arc<FieldsManager>,
        /// Whether this chunk came from the initial upstream-table snapshot.
        from_snapshot: bool,
        /// Snapshot epoch or log-store timestamp represented by this chunk.
        rw_timestamp: u64,
    },
}

#[derive(Clone)]
/// One raw chunk produced independently of any PostgreSQL `FETCH` format.
struct CursorDataChunk {
    /// Unformatted rows received from the local or distributed query executor.
    chunk: DataChunk,
    /// Metadata needed to format and project the raw rows for a later `FETCH`.
    kind: CursorDataChunkKind,
}

impl CursorDataChunk {
    /// Converts this raw chunk into rows for one PostgreSQL `FETCH`.
    fn into_pg_rows(
        self,
        formats: &[Format],
        session_data: &StaticSessionData,
    ) -> Result<Vec<Row>> {
        match self.kind {
            CursorDataChunkKind::Query { fields } => {
                let column_types = fields.iter().map(|field| field.data_type()).collect_vec();
                to_pg_rows(&column_types, self.chunk, formats, session_data)
            }
            CursorDataChunkKind::Subscription {
                fields,
                from_snapshot,
                rw_timestamp,
            } => {
                let (row_fields, row_formats) =
                    fields.get_row_stream_fields_and_formats(formats, from_snapshot)?;
                let column_types = row_fields
                    .iter()
                    .map(|field| field.data_type())
                    .collect_vec();
                let raw_formats = if row_formats.is_empty() {
                    &[][..]
                } else {
                    &row_formats[..column_types.len()]
                };
                to_pg_rows(&column_types, self.chunk, raw_formats, session_data)?
                    .into_iter()
                    .map(|row| {
                        let mut row = SubscriptionCursor::build_row(
                            row.take(),
                            (!from_snapshot).then_some(rw_timestamp),
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

#[derive(Clone)]
/// A non-row event that separates phases of cursor execution.
enum CursorDataChunkBarrier {
    /// The only query owned by a regular query cursor has completed.
    QueryEnd,
    /// A subscription query has started and exposes a new stream state to the foreground.
    SubscriptionQueryStarted {
        /// Whether the query reads the initial upstream-table snapshot.
        from_snapshot: bool,
        /// The snapshot epoch or log-store timestamp read by the query.
        rw_timestamp: u64,
        /// The next log-store timestamp expected after this query, when known.
        expected_timestamp: Option<u64>,
        /// The time at which query initialization began.
        init_query_timer: Instant,
        /// The output fields for chunks produced by the query.
        output_fields: Vec<Field>,
        /// The time at which this subscription query's retained data is no longer valid.
        expires_at: Instant,
    },
    /// A subscription query completed and the stream advanced to the supplied next epoch.
    SubscriptionNewEpoch {
        /// The timestamp from which the stream will search.
        seek_timestamp: u64,
        /// The exact next timestamp required to detect a retention gap, when known.
        expected_timestamp: Option<u64>,
    },
    /// No subscription log-store epoch is currently available.
    SubscriptionIdle,
    /// The upstream table schema changed before the next subscription query began.
    SchemaChanged,
}

#[derive(Clone)]
/// A raw chunk or cursor control barrier.
enum CursorDataChunkEvent {
    /// A data chunk whose rows can be formatted by the current `FETCH`.
    Chunk(CursorDataChunk),
    /// A control barrier produced between data chunks.
    Barrier(CursorDataChunkBarrier),
}

/// A regular query cursor's demand-driven raw data stream.
struct QueryCursorDataChunkStream {
    /// Boxed coroutine that owns the query stream and emits chunks followed by `QueryEnd`.
    inner: BoxStream<'static, std::result::Result<CursorDataChunkEvent, BoxedError>>,
}

impl QueryCursorDataChunkStream {
    /// Wraps an already-created query stream without adding another task or channel.
    fn new(query_stream: CursorQueryStream, fields: Vec<Field>) -> Self {
        Self {
            inner: Self::event_stream(query_stream, fields).boxed(),
        }
    }

    #[try_stream(ok = CursorDataChunkEvent, error = BoxedError)]
    async fn event_stream(mut query_stream: CursorQueryStream, fields: Vec<Field>) {
        let query_fields = Arc::new(fields);
        while let Some(chunk) = query_stream.next().await {
            yield CursorDataChunkEvent::Chunk(CursorDataChunk {
                chunk: chunk?,
                kind: CursorDataChunkKind::Query {
                    fields: query_fields.clone(),
                },
            });
        }
        yield CursorDataChunkEvent::Barrier(CursorDataChunkBarrier::QueryEnd);
    }
}

impl Stream for QueryCursorDataChunkStream {
    type Item = std::result::Result<CursorDataChunkEvent, BoxedError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.get_mut().inner.poll_next_unpin(cx)
    }
}

/// A subscription cursor's demand-driven raw data stream.
struct SubscriptionCursorDataChunkStream {
    /// Boxed coroutine that owns all query execution and state transitions for the subscription.
    inner: BoxStream<'static, std::result::Result<CursorDataChunkEvent, BoxedError>>,
}

impl SubscriptionCursorDataChunkStream {
    /// Owns the complete subscription execution coroutine without spawning another task.
    #[allow(clippy::too_many_arguments)]
    fn new(
        subscription: Arc<SubscriptionCatalog>,
        dependent_table_id: TableId,
        handler_context: SubscriptionCursorHandlerContext,
        fields_manager: FieldsManager,
        state: SubscriptionCursorDataChunkStreamState,
        cursor_metrics: Arc<CursorMetrics>,
        query_shutdown_tx: ShutdownSender,
        query_shutdown_rx: ShutdownToken,
    ) -> Self {
        Self {
            inner: Self::event_stream(
                subscription,
                dependent_table_id,
                handler_context,
                fields_manager,
                state,
                cursor_metrics,
                query_shutdown_tx,
                query_shutdown_rx,
            )
            .boxed(),
        }
    }

    #[try_stream(ok = CursorDataChunkEvent, error = BoxedError)]
    #[allow(clippy::too_many_arguments)]
    async fn event_stream(
        subscription: Arc<SubscriptionCatalog>,
        dependent_table_id: TableId,
        handler_context: SubscriptionCursorHandlerContext,
        mut fields_manager: FieldsManager,
        mut state: SubscriptionCursorDataChunkStreamState,
        cursor_metrics: Arc<CursorMetrics>,
        query_shutdown_tx: ShutdownSender,
        query_shutdown_rx: ShutdownToken,
    ) {
        loop {
            let current_state =
                mem::replace(&mut state, SubscriptionCursorDataChunkStreamState::Invalid);
            match current_state {
                SubscriptionCursorDataChunkStreamState::InitLogStoreQuery {
                    seek_timestamp,
                    expected_timestamp,
                } => {
                    let handler_args = handler_context.handler_args()?;
                    let (rw_timestamp, next_expected_timestamp) =
                        match SubscriptionCursor::get_next_rw_timestamp(
                            seek_timestamp,
                            dependent_table_id,
                            expected_timestamp,
                            handler_args,
                            &subscription,
                        )
                        .await?
                        {
                            (Some(rw_timestamp), next_expected_timestamp) => {
                                (rw_timestamp, next_expected_timestamp)
                            }
                            (None, _) => {
                                state = SubscriptionCursorDataChunkStreamState::InitLogStoreQuery {
                                    seek_timestamp,
                                    expected_timestamp,
                                };
                                yield CursorDataChunkEvent::Barrier(
                                    CursorDataChunkBarrier::SubscriptionIdle,
                                );
                                let session = handler_context.handler_args()?.session;
                                session
                                    .env
                                    .hummock_snapshot_manager()
                                    .wait_table_change_log_notification(
                                        dependent_table_id,
                                        seek_timestamp,
                                    )
                                    .await?;
                                continue;
                            }
                        };

                    let handler_args = handler_context.handler_args()?;
                    let snapshot = ReadSnapshot::FrontendPinned {
                        snapshot: handler_args
                            .session
                            .env
                            .hummock_snapshot_manager()
                            .acquire(),
                    };
                    let (query_stream, init_query_timer, catalog) =
                        SubscriptionCursor::initiate_query(
                            Some(rw_timestamp),
                            dependent_table_id,
                            handler_args,
                            query_shutdown_tx.clone(),
                            query_shutdown_rx.clone(),
                            snapshot,
                        )
                        .await?;
                    let schema_changed = fields_manager.try_refill_fields(&catalog);
                    state = SubscriptionCursorDataChunkStreamState::Fetch {
                        from_snapshot: false,
                        rw_timestamp,
                        query_stream,
                        expected_timestamp: next_expected_timestamp,
                        init_query_timer,
                    };
                    yield CursorDataChunkEvent::Barrier(
                        CursorDataChunkBarrier::SubscriptionQueryStarted {
                            from_snapshot: false,
                            rw_timestamp,
                            expected_timestamp: next_expected_timestamp,
                            init_query_timer,
                            output_fields: fields_manager.get_output_fields(),
                            expires_at: Instant::now()
                                + Duration::from_secs(subscription.retention_seconds),
                        },
                    );
                    if schema_changed {
                        yield CursorDataChunkEvent::Barrier(CursorDataChunkBarrier::SchemaChanged);
                    }
                }
                SubscriptionCursorDataChunkStreamState::Fetch {
                    from_snapshot,
                    rw_timestamp,
                    mut query_stream,
                    expected_timestamp,
                    init_query_timer,
                } => match query_stream.next().await {
                    Some(Ok(chunk)) => {
                        state = SubscriptionCursorDataChunkStreamState::Fetch {
                            from_snapshot,
                            rw_timestamp,
                            query_stream,
                            expected_timestamp,
                            init_query_timer,
                        };
                        yield CursorDataChunkEvent::Chunk(CursorDataChunk {
                            chunk,
                            kind: CursorDataChunkKind::Subscription {
                                fields: Arc::new(fields_manager.clone()),
                                from_snapshot,
                                rw_timestamp,
                            },
                        });
                    }
                    Some(Err(error)) => Err(error)?,
                    None => {
                        cursor_metrics
                            .subscription_cursor_query_duration
                            .with_label_values(&[&subscription.name])
                            .observe(init_query_timer.elapsed().as_millis() as _);
                        let (seek_timestamp, expected_timestamp) =
                            if let Some(expected_timestamp) = expected_timestamp {
                                (expected_timestamp, Some(expected_timestamp))
                            } else {
                                (rw_timestamp + 1, None)
                            };
                        state = SubscriptionCursorDataChunkStreamState::InitLogStoreQuery {
                            seek_timestamp,
                            expected_timestamp,
                        };
                        yield CursorDataChunkEvent::Barrier(
                            CursorDataChunkBarrier::SubscriptionNewEpoch {
                                seek_timestamp,
                                expected_timestamp,
                            },
                        );
                    }
                },
                SubscriptionCursorDataChunkStreamState::Invalid => return Ok(()),
            }
        }
    }
}

impl Stream for SubscriptionCursorDataChunkStream {
    type Item = std::result::Result<CursorDataChunkEvent, BoxedError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.get_mut().inner.poll_next_unpin(cx)
    }
}

/// A raw event retained until a successful `FETCH` commits past it.
struct CachedCursorDataChunkEvent {
    /// Raw chunk or control barrier already polled from the irreversible underlying stream.
    event: CursorDataChunkEvent,
    /// Number of leading formatted output rows already committed for a chunk; zero for a barrier.
    row_offset_in_chunk: usize,
}

/// Rows formatted for the current `FETCH` and their next raw-chunk offset.
struct FormattedCursorDataChunk {
    /// Formatted-row offset immediately after the last row tentatively yielded by this `FETCH`.
    next_offset: usize,
    /// Formatted rows in this raw chunk that have not yet been yielded by this `FETCH`.
    rows: VecDeque<Row>,
}

/// Tentative row and raw-event progress shared by every kind of `FETCH` command.
struct CursorPgResponseFetchStateInner {
    /// PostgreSQL result formats requested by this `FETCH`.
    formats: Vec<Format>,
    /// Session settings captured when this `FETCH` began and needed for row formatting.
    session_data: StaticSessionData,
    /// Tentative index of the cached event currently being processed or to be processed next.
    next_event_index: usize,
    /// Current raw chunk formatted using this `FETCH`'s formats, if one is being processed.
    current_formatted_chunk: Option<FormattedCursorDataChunk>,
    /// Number of rows tentatively returned to this `FETCH` so far.
    yielded_rows: usize,
    /// Whether cursor-specific boundary handling has ended this `FETCH`.
    finished: bool,
}

impl CursorPgResponseFetchStateInner {
    /// Creates uncommitted progress for a new `FETCH` command.
    fn new(formats: &[Format], session: &SessionImpl) -> Self {
        Self {
            formats: formats.to_vec(),
            session_data: StaticSessionData {
                timezone: session.config().timezone(),
            },
            next_event_index: 0,
            current_formatted_chunk: None,
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
        inner: CursorPgResponseFetchStateInner,
    },
    /// Tentative progress and raw-stream metadata for a subscription cursor.
    Subscription {
        /// Row and raw-event progress shared with regular query cursors.
        inner: CursorPgResponseFetchStateInner,
        /// Whether an idle `FETCH` waits for the raw stream to receive future data.
        wait_for_data: bool,
        /// Producer position to commit after this `FETCH` succeeds.
        next_subscription_state: Option<SubscriptionCursorState>,
        /// Output fields to commit after this `FETCH` succeeds.
        next_output_fields: Option<Vec<Field>>,
        /// Retention deadline to commit after this `FETCH` succeeds.
        next_expires_at: Option<Instant>,
    },
}

impl CursorPgResponseFetchState {
    /// Returns the tentative progress shared by both cursor kinds.
    fn inner(&self) -> &CursorPgResponseFetchStateInner {
        match self {
            Self::Query { inner } | Self::Subscription { inner, .. } => inner,
        }
    }

    /// Returns mutable tentative progress shared by both cursor kinds.
    fn inner_mut(&mut self) -> &mut CursorPgResponseFetchStateInner {
        match self {
            Self::Query { inner } | Self::Subscription { inner, .. } => inner,
        }
    }

    /// Returns whether an idle subscription `FETCH` should wait for newly produced data.
    fn wait_for_data(&self) -> bool {
        match self {
            Self::Query { .. } => unreachable!("query fetch cannot wait for new data"),
            Self::Subscription { wait_for_data, .. } => *wait_for_data,
        }
    }

    /// Records metadata for a newly started subscription query.
    fn update_when_subscription_query_started(
        &mut self,
        from_snapshot: bool,
        rw_timestamp: u64,
        expected_timestamp: Option<u64>,
        init_query_timer: Instant,
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
                *next_subscription_state = Some(SubscriptionCursorState::Fetch {
                    from_snapshot,
                    rw_timestamp,
                    expected_timestamp,
                    init_query_timer,
                });
                *next_output_fields = Some(output_fields);
                *next_expires_at = Some(expires_at);
            }
        }
    }

    /// Records the next subscription log-store epoch.
    fn update_when_subscription_new_epoch(
        &mut self,
        seek_timestamp: u64,
        expected_timestamp: Option<u64>,
    ) {
        match self {
            Self::Query { .. } => unreachable!("query fetch cannot update subscription state"),
            Self::Subscription {
                next_subscription_state,
                ..
            } => {
                *next_subscription_state = Some(SubscriptionCursorState::InitLogStoreQuery {
                    seek_timestamp,
                    expected_timestamp,
                });
            }
        }
    }
}

/// State shared by concrete query and subscription PostgreSQL response streams.
struct CursorPgResponseStreamInner<S> {
    /// Irreversible source of raw chunks and ordered cursor control barriers.
    data_stream: S,
    /// Raw events polled from `data_stream` but not yet fully committed by a successful `FETCH`.
    cached_events: VecDeque<CachedCursorDataChunkEvent>,
    /// Tentative progress for the active `FETCH`, or `None` between commands.
    fetch_state: Option<CursorPgResponseFetchState>,
    /// Output fields committed by successful preceding `FETCH` commands.
    output_fields: Vec<Field>,
    /// Whether this response stream has entered an unrecoverable terminal state.
    failed: bool,
}

/// One item produced by the shared response-stream polling core.
enum CursorPgResponsePollItem {
    /// A formatted PostgreSQL row.
    Row(Row),
    /// A cursor-specific control barrier interpreted by the concrete response stream.
    Barrier(CursorDataChunkBarrier),
    /// The underlying raw data stream ended.
    DataChunkStreamEnd,
}

impl<S> CursorPgResponseStreamInner<S> {
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

    /// Returns whether the raw data stream reported an internal failure.
    fn is_failed(&self) -> bool {
        self.failed
    }

    /// Returns whether the active `FETCH` has reached its command boundary.
    fn is_fetch_finished(&self) -> bool {
        self.fetch_state
            .as_ref()
            .is_some_and(|state| state.inner().finished)
    }

    /// Marks the active `FETCH` as complete without committing its tentative position yet.
    fn finish_fetch(&mut self) {
        self.fetch_state
            .as_mut()
            .expect("response stream must be inside a FETCH")
            .inner_mut()
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
    ///
    /// Fully consumed cached events are removed. If the active chunk was only partly consumed,
    /// its raw event is retained and its committed `row_offset` is advanced so the next `FETCH`
    /// can reformat the chunk and resume at the first uncommitted row.
    fn commit_fetch(&mut self) -> Option<CursorPgResponseFetchState> {
        let mut fetch_state = self.fetch_state.take()?;
        let next_event_index = {
            let inner = fetch_state.inner_mut();
            if let Some(chunk) = inner.current_formatted_chunk.take() {
                let event = self
                    .cached_events
                    .get_mut(inner.next_event_index)
                    .expect("current formatted chunk must have a cached raw chunk");
                debug_assert!(matches!(event.event, CursorDataChunkEvent::Chunk(_)));
                if chunk.rows.is_empty() {
                    inner.next_event_index += 1;
                } else {
                    event.row_offset_in_chunk = chunk.next_offset;
                }
            }
            inner.next_event_index
        };
        drop(self.cached_events.drain(..next_event_index));
        Some(fetch_state)
    }

    /// Rolls back the current `FETCH` while retaining every raw event for the next command.
    fn abort_fetch(&mut self) {
        self.fetch_state = None;
    }
}

impl<S> CursorPgResponseStreamInner<S>
where
    S: Stream<Item = std::result::Result<CursorDataChunkEvent, BoxedError>> + Unpin,
{
    /// Polls one formatted row, control barrier, or raw-stream termination.
    fn poll_next_item(&mut self, cx: &mut Context<'_>) -> Poll<Result<CursorPgResponsePollItem>> {
        loop {
            let Some(fetch_state) = self.fetch_state.as_mut() else {
                return Poll::Ready(Err(ErrorCode::InternalError(
                    "cursor response stream polled outside a FETCH".to_owned(),
                )
                .into()));
            };

            {
                // The first level cache: consume the current formatted chunk's remaining rows
                // before poll the next chunk.
                let inner = fetch_state.inner_mut();
                if let Some(chunk) = inner.current_formatted_chunk.as_mut() {
                    if let Some(row) = chunk.rows.pop_front() {
                        chunk.next_offset += 1;
                        inner.yielded_rows += 1;
                        return Poll::Ready(Ok(CursorPgResponsePollItem::Row(row)));
                    } else {
                        // Dispose the completely-consumed formatted chunk and try to find the next raw chunk
                        // in the local cache, which is the second level cache.
                        fetch_state.inner_mut().current_formatted_chunk.take();
                        fetch_state.inner_mut().next_event_index += 1
                    }
                }
            }

            let event_index = fetch_state.inner().next_event_index;
            // All cached events have been consumed, poll a new one.
            if event_index == self.cached_events.len() {
                match self.data_stream.poll_next_unpin(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Some(Ok(event))) => {
                        self.cached_events.push_back(CachedCursorDataChunkEvent {
                            event,
                            row_offset_in_chunk: 0,
                        });
                        continue;
                    }
                    Poll::Ready(Some(Err(error))) => {
                        self.failed = true;
                        fetch_state.inner_mut().finished = true;
                        return Poll::Ready(Err(error.into()));
                    }
                    Poll::Ready(None) => {
                        fetch_state.inner_mut().finished = true;
                        return Poll::Ready(Ok(CursorPgResponsePollItem::DataChunkStreamEnd));
                    }
                }
            }
            debug_assert!(event_index < self.cached_events.len());
            let event = self.cached_events[event_index].event.clone();
            let row_offset = self.cached_events[event_index].row_offset_in_chunk;

            match event {
                CursorDataChunkEvent::Chunk(data) => {
                    let rows = match data.into_pg_rows(
                        &fetch_state.inner().formats,
                        &fetch_state.inner().session_data,
                    ) {
                        Ok(rows) => rows,
                        Err(error) => {
                            return Poll::Ready(Err(error));
                        }
                    };
                    fetch_state.inner_mut().current_formatted_chunk =
                        Some(FormattedCursorDataChunk {
                            next_offset: row_offset,
                            rows: rows.into_iter().skip(row_offset).collect(),
                        });
                }
                CursorDataChunkEvent::Barrier(barrier) => {
                    // Barrier is instantly consumed, not like Chunk, which we keep consuming it
                    // until all rows within it are consumed.
                    fetch_state.inner_mut().next_event_index += 1;
                    return Poll::Ready(Ok(CursorPgResponsePollItem::Barrier(barrier)));
                }
            }
        }
    }
}

/// A regular query cursor's rollback-safe PostgreSQL response stream.
struct QueryCursorPgResponseStream {
    /// Shared raw-event cache and per-`FETCH` tentative progress.
    inner: CursorPgResponseStreamInner<QueryCursorDataChunkStream>,
}

impl QueryCursorPgResponseStream {
    /// Creates the foreground stream for a regular query cursor.
    fn new(data_stream: QueryCursorDataChunkStream, output_fields: Vec<Field>) -> Self {
        Self {
            inner: CursorPgResponseStreamInner::new(data_stream, output_fields),
        }
    }

    /// Returns the output fields committed by previous `FETCH` commands.
    fn fields(&self) -> Vec<Field> {
        self.inner.fields()
    }

    /// Begins tentative progress for one regular query `FETCH` command.
    fn begin_fetch(&mut self, formats: &[Format], session: &SessionImpl) {
        self.inner.fetch_state = Some(CursorPgResponseFetchState::Query {
            inner: CursorPgResponseFetchStateInner::new(formats, session),
        });
    }

    /// Commits the current `FETCH` position.
    fn commit_fetch(&mut self) {
        _ = self.inner.commit_fetch();
    }

    /// Rolls back the current `FETCH` while retaining every raw event for the next command.
    fn abort_fetch(&mut self) {
        self.inner.abort_fetch();
    }
}

impl Stream for QueryCursorPgResponseStream {
    type Item = Result<Row>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.inner.is_fetch_finished() {
            return Poll::Ready(None);
        }
        match this.inner.poll_next_item(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(error)) => Poll::Ready(Some(Err(error))),
            Poll::Ready(Ok(CursorPgResponsePollItem::Row(row))) => Poll::Ready(Some(Ok(row))),
            Poll::Ready(Ok(CursorPgResponsePollItem::Barrier(
                CursorDataChunkBarrier::QueryEnd,
            ))) => {
                this.inner.finish_fetch();
                Poll::Ready(None)
            }
            Poll::Ready(Ok(CursorPgResponsePollItem::Barrier(_))) => {
                this.inner.finish_fetch();
                Poll::Ready(Some(Err(ErrorCode::InternalError(
                    "query cursor received a non-query barrier".to_owned(),
                )
                .into())))
            }
            Poll::Ready(Ok(CursorPgResponsePollItem::DataChunkStreamEnd)) => {
                if this.inner.failed {
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
    /// Shared raw-event cache, active per-`FETCH` state, and committed output fields.
    inner: CursorPgResponseStreamInner<SubscriptionCursorDataChunkStream>,
    /// Logical subscription position committed by the most recent successful `FETCH`.
    subscription_state: SubscriptionCursorState,
    /// Retention deadline associated with the committed subscription position.
    expires_at: Instant,
    /// Whether the raw stream is specifically waiting for the next available epoch.
    ///
    /// This distinguishes that idle wait from an ordinary `Poll::Pending` produced while an
    /// active query is still executing.
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
            inner: CursorPgResponseStreamInner::new(data_stream, output_fields),
            subscription_state,
            expires_at,
            subscription_idle: false,
        }
    }

    /// Returns the output fields committed by previous `FETCH` commands.
    fn fields(&self) -> Vec<Field> {
        self.inner.fields()
    }

    /// Returns the subscription state committed by the foreground stream.
    fn subscription_state(&self) -> &SubscriptionCursorState {
        &self.subscription_state
    }

    /// Returns whether the subscription's current logical position has expired.
    fn is_expired(&self, now: Instant) -> bool {
        now > self.expires_at
    }

    /// Returns whether the raw data stream reported a terminal internal failure.
    fn is_failed(&self) -> bool {
        self.inner.is_failed()
    }

    /// Begins tentative subscription `FETCH` progress.
    fn begin_fetch(&mut self, formats: &[Format], session: &SessionImpl, wait_for_data: bool) {
        self.inner.fetch_state = Some(CursorPgResponseFetchState::Subscription {
            inner: CursorPgResponseFetchStateInner::new(formats, session),
            wait_for_data,
            next_subscription_state: None,
            next_output_fields: None,
            next_expires_at: None,
        });
    }

    /// Commits the current `FETCH` position and its ordered raw-stream metadata.
    fn commit_fetch(&mut self) {
        let Some(fetch_state) = self.inner.commit_fetch() else {
            return;
        };
        let CursorPgResponseFetchState::Subscription {
            next_subscription_state,
            next_output_fields,
            next_expires_at,
            ..
        } = fetch_state
        else {
            unreachable!("subscription response stream must own subscription fetch state");
        };
        if let Some(state) = next_subscription_state {
            self.subscription_state = state;
        }
        if let Some(output_fields) = next_output_fields {
            self.inner.output_fields = output_fields;
        }
        if let Some(expires_at) = next_expires_at {
            self.expires_at = expires_at;
        }
    }

    /// Rolls back the current `FETCH` while retaining every raw event for the next command.
    fn abort_fetch(&mut self) {
        self.inner.abort_fetch();
    }
}

impl Stream for SubscriptionCursorPgResponseStream {
    type Item = Result<Row>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            if this.inner.is_fetch_finished() {
                return Poll::Ready(None);
            }
            match this.inner.poll_next_item(cx) {
                Poll::Pending if this.subscription_idle => {
                    let fetch_state = this.inner.fetch_state();
                    if fetch_state.inner().yielded_rows > 0 || !fetch_state.wait_for_data() {
                        this.inner.finish_fetch();
                        return Poll::Ready(None);
                    }
                    return Poll::Pending;
                }
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(error)) => {
                    if this.inner.failed {
                        this.subscription_state = SubscriptionCursorState::Invalid;
                    }
                    return Poll::Ready(Some(Err(error)));
                }
                Poll::Ready(Ok(CursorPgResponsePollItem::Row(row))) => {
                    this.subscription_idle = false;
                    return Poll::Ready(Some(Ok(row)));
                }
                Poll::Ready(Ok(CursorPgResponsePollItem::Barrier(barrier))) => {
                    this.subscription_idle = false;
                    let should_finish = match barrier {
                        CursorDataChunkBarrier::QueryEnd
                        | CursorDataChunkBarrier::SchemaChanged => true,
                        CursorDataChunkBarrier::SubscriptionQueryStarted {
                            from_snapshot,
                            rw_timestamp,
                            expected_timestamp,
                            init_query_timer,
                            output_fields,
                            expires_at,
                        } => {
                            this.inner
                                .fetch_state_mut()
                                .update_when_subscription_query_started(
                                    from_snapshot,
                                    rw_timestamp,
                                    expected_timestamp,
                                    init_query_timer,
                                    output_fields,
                                    expires_at,
                                );
                            false
                        }
                        CursorDataChunkBarrier::SubscriptionNewEpoch {
                            seek_timestamp,
                            expected_timestamp,
                        } => {
                            this.inner
                                .fetch_state_mut()
                                .update_when_subscription_new_epoch(
                                    seek_timestamp,
                                    expected_timestamp,
                                );
                            this.inner.fetch_state().inner().yielded_rows > 0
                        }
                        CursorDataChunkBarrier::SubscriptionIdle => {
                            this.subscription_idle = true;
                            let fetch_state = this.inner.fetch_state();
                            fetch_state.inner().yielded_rows > 0 || !fetch_state.wait_for_data()
                        }
                    };
                    if should_finish {
                        this.inner.finish_fetch();
                        return Poll::Ready(None);
                    }
                }
                Poll::Ready(Ok(CursorPgResponsePollItem::DataChunkStreamEnd)) => {
                    if this.inner.failed {
                        return Poll::Ready(Some(Err(ErrorCode::InternalError(
                            "Cursor data stream has terminated with an error; close and recreate the cursor"
                                .to_owned(),
                        )
                        .into())));
                    }
                    this.inner.failed = true;
                    this.subscription_state = SubscriptionCursorState::Invalid;
                    return Poll::Ready(Some(Err(ErrorCode::InternalError(
                        "Subscription cursor data stream terminated unexpectedly; close and recreate the cursor"
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
    /// Sender registered with the session so a PostgreSQL `CancelRequest` can cancel this `FETCH`.
    cancel_tx: ShutdownSender,
    /// Token awaited by the active `FETCH` loop.
    cancel_rx: ShutdownToken,
}

impl FetchCursorCancelHandle {
    /// Creates an uncanceled per-`FETCH` handle.
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

/// A regular query cursor whose raw stream survives individual `FETCH` cancellation.
pub struct QueryCursor {
    /// Declared first so cursor drop signals shutdown before the raw stream is dropped.
    lifecycle: CursorLifecycle,
    /// Owns raw-event caching and tentative per-`FETCH` progress.
    pg_response_stream: QueryCursorPgResponseStream,
}

impl QueryCursor {
    /// Creates a lifecycle, schedules a planned query, and wraps its raw chunk stream.
    pub(crate) async fn new(
        session: Arc<SessionImpl>,
        plan_fragmenter_result: BatchPlanFragmenterResult,
    ) -> Result<Self> {
        let lifecycle = CursorLifecycle::new(session.get_cursor_manager().session_shutdown_token());
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
        let chunk_stream = QueryCursorDataChunkStream::new(query_stream, fields.clone());
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
/// Foreground-visible subscription position received from ordered raw-stream barriers.
enum SubscriptionCursorState {
    /// The cursor is looking for the next available subscription log-store epoch.
    InitLogStoreQuery {
        /// The timestamp from which the stream will search.
        seek_timestamp: u64,

        /// When present, the next available timestamp must exactly match this value.
        expected_timestamp: Option<u64>,
    },
    /// The cursor has started a snapshot or log-store query at this logical position.
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
    /// The raw stream reported an unrecoverable error.
    Invalid,
}

impl Display for SubscriptionCursorState {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
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

/// The complete state machine owned by a subscription cursor's raw stream.
enum SubscriptionCursorDataChunkStreamState {
    /// Searches for the next available subscription log-store epoch.
    InitLogStoreQuery {
        /// The timestamp from which the stream searches.
        seek_timestamp: u64,
        /// An exact timestamp required to detect retention gaps.
        expected_timestamp: Option<u64>,
    },
    /// Streams one active snapshot or log-store query into raw cursor events.
    Fetch {
        /// Whether this is the initial upstream-table snapshot.
        from_snapshot: bool,
        /// The snapshot epoch or log-store timestamp read by the query.
        rw_timestamp: u64,
        /// The query stream owned by the cursor's raw stream.
        query_stream: CursorQueryStream,
        /// The next timestamp expected after this query, when known.
        expected_timestamp: Option<u64>,
        /// The query initialization time used by cursor metrics.
        init_query_timer: Instant,
    },
    /// A temporary sentinel used while moving the current state through an async transition.
    Invalid,
}

/// Session context needed to plan later subscription batches without retaining the session while
/// the cursor stream is dormant.
struct SubscriptionCursorHandlerContext {
    /// Weak session reference so a dormant cursor cannot keep a terminated session alive.
    session: Weak<SessionImpl>,
    /// Original SQL text retained for planning later subscription queries.
    sql: Arc<str>,
    /// Normalized SQL text retained for query diagnostics and metrics.
    normalized_sql: String,
    /// Statement options retained for planning later subscription queries.
    with_options: WithOptions,
}

impl SubscriptionCursorHandlerContext {
    fn new(handler_args: &HandlerArgs) -> Self {
        Self {
            session: Arc::downgrade(&handler_args.session),
            sql: handler_args.sql.clone(),
            normalized_sql: handler_args.normalized_sql.clone(),
            with_options: handler_args.with_options.clone(),
        }
    }

    fn handler_args(&self) -> Result<HandlerArgs> {
        let session = self.session.upgrade().ok_or_else(|| {
            ErrorCode::InternalError("session ended while polling subscription cursor".to_owned())
        })?;
        Ok(HandlerArgs {
            session,
            sql: self.sql.clone(),
            normalized_sql: self.normalized_sql.clone(),
            with_options: self.with_options.clone(),
        })
    }
}

/// A subscription cursor with foreground state committed only from raw-stream barriers.
pub struct SubscriptionCursor {
    /// Declared first so cursor drop signals shutdown before its raw stream is dropped.
    lifecycle: CursorLifecycle,
    /// Name under which this cursor is registered in its session.
    cursor_name: String,
    /// Catalog entry for the subscription followed by this cursor.
    subscription: Arc<SubscriptionCatalog>,
    /// Upstream table read by snapshot and log-store queries.
    dependent_table_id: TableId,
    /// Raw-event cache, tentative progress, and foreground-committed subscription metadata.
    pg_response_stream: SubscriptionCursorPgResponseStream,
    /// Metrics updated by this cursor's query and `FETCH` lifecycle.
    cursor_metrics: Arc<CursorMetrics>,
    /// Completion time of the most recent successful `FETCH`.
    last_fetch: Instant,
}

impl SubscriptionCursor {
    /// Declares a subscription cursor and captures a `FULL` cursor's snapshot immediately.
    pub async fn new(
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
        );
        let query_shutdown_tx = lifecycle.query_shutdown_sender();
        let query_shutdown_rx = lifecycle.query_shutdown_token();
        let (stream_state, cursor_state, fields_manager) =
            if let Some(start_timestamp) = start_timestamp {
                let table_catalog = handler_args.session.get_table_by_id(dependent_table_id)?;
                (
                    SubscriptionCursorDataChunkStreamState::InitLogStoreQuery {
                        seek_timestamp: start_timestamp,
                        expected_timestamp: None,
                    },
                    SubscriptionCursorState::InitLogStoreQuery {
                        seek_timestamp: start_timestamp,
                        expected_timestamp: None,
                    },
                    FieldsManager::new(&table_catalog),
                )
            } else {
                // `FULL` is the only subscription mode represented by `None`. Pin its snapshot
                // during `DECLARE`; every `SINCE` mode stays in `InitLogStoreQuery` without one.
                let snapshot = handler_args.session.pinned_snapshot();
                let pinned_epoch = Self::snapshot_epoch(&snapshot, dependent_table_id)?;
                let (query_stream, init_query_timer, table_catalog) = Self::initiate_query(
                    None,
                    dependent_table_id,
                    handler_args.clone(),
                    query_shutdown_tx.clone(),
                    query_shutdown_rx.clone(),
                    snapshot,
                )
                .await?;
                (
                    SubscriptionCursorDataChunkStreamState::Fetch {
                        from_snapshot: true,
                        rw_timestamp: pinned_epoch,
                        query_stream,
                        expected_timestamp: None,
                        init_query_timer,
                    },
                    SubscriptionCursorState::Fetch {
                        from_snapshot: true,
                        rw_timestamp: pinned_epoch,
                        expected_timestamp: None,
                        init_query_timer,
                    },
                    FieldsManager::new(&table_catalog),
                )
            };

        let output_fields = fields_manager.get_output_fields();
        let expires_at = Instant::now() + Duration::from_secs(subscription.retention_seconds);
        let chunk_stream = SubscriptionCursorDataChunkStream::new(
            subscription.clone(),
            dependent_table_id,
            SubscriptionCursorHandlerContext::new(handler_args),
            fields_manager,
            stream_state,
            cursor_metrics.clone(),
            query_shutdown_tx,
            query_shutdown_rx,
        );

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

    fn snapshot_epoch(snapshot: &ReadSnapshot, dependent_table_id: TableId) -> Result<u64> {
        match snapshot {
            ReadSnapshot::FrontendPinned { snapshot, .. } => snapshot
                .version()
                .state_table_info
                .info()
                .get(&dependent_table_id)
                .ok_or_else(|| anyhow!("dependent_table_id {dependent_table_id} not exists").into())
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
        }
    }

    /// Executes one SQL `FETCH` while preserving raw-stream progress across timeout or cancellation.
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
        let mut ans = Vec::with_capacity(std::cmp::min(100, count) as usize);
        while ans.len() < count as usize {
            let fetch_row_timer = Instant::now();
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
                    Some(Ok(row)) => {
                        self.cursor_metrics
                            .subscription_cursor_fetch_duration
                            .with_label_values(&[&self.subscription.name])
                            .observe(fetch_row_timer.elapsed().as_millis() as _);
                        ans.push(row);
                    }
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
            // Only used to return generated plans, so rw_timestamp is meaningless.
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

    /// Formats the foreground-committed stream position for diagnostics.
    pub fn state_info_string(&self) -> String {
        self.pg_response_stream.subscription_state().to_string()
    }
}

/// Owns every named cursor and the session-wide cursor shutdown signal.
pub struct CursorManager {
    /// Session-local cursors, locked across each `FETCH` to serialize cursor access.
    cursor_map: tokio::sync::Mutex<HashMap<String, Cursor>>,
    /// Metrics shared by all cursors in this frontend process.
    cursor_metrics: Arc<CursorMetrics>,
    /// Sender canceled when this frontend session begins shutting down.
    session_shutdown_tx: ShutdownSender,
    /// Token cloned into cursors so active `FETCH` commands observe session shutdown.
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

    /// Returns a token observed by every cursor foreground in this session.
    pub fn session_shutdown_token(&self) -> ShutdownToken {
        self.session_shutdown_rx.clone()
    }

    /// Signals active `FETCH` commands, then explicitly drops all cursor-owned streams.
    ///
    /// An active `FETCH` holds the cursor-map lock while it awaits its response stream. In that
    /// case cancellation releases the lock and this cleanup task clears the map afterward.
    pub fn shutdown(self: &Arc<Self>) {
        if !self.session_shutdown_tx.cancel() {
            return;
        }
        if let Ok(mut cursor_map) = self.cursor_map.try_lock() {
            cursor_map.clear();
            return;
        }
        let cursor_manager = self.clone();
        tokio::spawn(async move {
            cursor_manager.cursor_map.lock().await.clear();
        });
    }

    /// Completes session shutdown only after all cursor-owned streams have been dropped.
    pub async fn shutdown_and_wait(&self) {
        self.session_shutdown_tx.cancel();
        self.cursor_map.lock().await.clear();
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
        )
        .await?;
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

    use futures::FutureExt;
    use risingwave_common::array::DataChunkTestExt;
    use risingwave_sqlparser::parser::Parser;
    use tokio::sync::{mpsc, oneshot};

    use super::*;

    impl QueryCursorDataChunkStream {
        #[try_stream(ok = CursorDataChunkEvent, error = BoxedError)]
        async fn events_for_test(events: Vec<CursorDataChunkEvent>, started: Arc<AtomicBool>) {
            started.store(true, Ordering::Relaxed);
            for event in events {
                yield event;
            }
        }

        /// Creates a synthetic query stream that begins only when polled.
        fn for_test(events: Vec<CursorDataChunkEvent>) -> (Self, Arc<AtomicBool>) {
            let started = Arc::new(AtomicBool::new(false));
            (
                Self {
                    inner: Self::events_for_test(events, started.clone()).boxed(),
                },
                started,
            )
        }

        #[try_stream(ok = CursorDataChunkEvent, error = BoxedError)]
        async fn pending_for_test(started: Arc<AtomicBool>, resume_rx: oneshot::Receiver<()>) {
            started.store(true, Ordering::Relaxed);
            resume_rx.await.unwrap();
            yield CursorDataChunkEvent::Barrier(CursorDataChunkBarrier::QueryEnd);
        }
    }

    impl SubscriptionCursorDataChunkStream {
        #[try_stream(ok = CursorDataChunkEvent, error = BoxedError)]
        async fn events_for_test(events: Vec<CursorDataChunkEvent>, started: Arc<AtomicBool>) {
            started.store(true, Ordering::Relaxed);
            for event in events {
                yield event;
            }
            std::future::pending::<()>().await;
        }

        /// Creates a synthetic subscription stream that begins only when polled.
        fn for_test(events: Vec<CursorDataChunkEvent>) -> (Self, Arc<AtomicBool>) {
            let started = Arc::new(AtomicBool::new(false));
            (
                Self {
                    inner: Self::events_for_test(events, started.clone()).boxed(),
                },
                started,
            )
        }
    }

    fn handler_args(session: Arc<SessionImpl>) -> HandlerArgs {
        let sql: Arc<str> = "select 1".into();
        let statement = Parser::parse_exactly_one(&sql).unwrap();
        HandlerArgs::new(session, &statement, sql).unwrap()
    }

    fn test_lifecycle() -> CursorLifecycle {
        let (_, session_shutdown_rx) = ShutdownToken::new();
        CursorLifecycle::new(session_shutdown_rx)
    }

    fn query_data_stream() -> (
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
        let lifecycle = test_lifecycle();
        let (stream, started) = QueryCursorDataChunkStream::for_test(events);
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
    ) -> (
        CursorLifecycle,
        SubscriptionCursorDataChunkStream,
        Arc<AtomicBool>,
    ) {
        let fields = Arc::new(subscription_fields());
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
        let mut events = vec![];
        if !from_snapshot {
            events.push(CursorDataChunkEvent::Barrier(
                CursorDataChunkBarrier::SubscriptionQueryStarted {
                    from_snapshot,
                    rw_timestamp: 1 << 16,
                    expected_timestamp: None,
                    init_query_timer: Instant::now(),
                    output_fields: fields.get_output_fields(),
                    expires_at: Instant::now() + Duration::from_secs(60),
                },
            ));
        }
        events.extend([
            CursorDataChunkEvent::Chunk(CursorDataChunk {
                chunk,
                kind: CursorDataChunkKind::Subscription {
                    fields,
                    from_snapshot,
                    rw_timestamp: 1 << 16,
                },
            }),
            CursorDataChunkEvent::Barrier(CursorDataChunkBarrier::SubscriptionNewEpoch {
                seek_timestamp: (1 << 16) + 1,
                expected_timestamp: None,
            }),
        ]);
        let lifecycle = test_lifecycle();
        let (stream, started) = SubscriptionCursorDataChunkStream::for_test(events);
        (lifecycle, stream, started)
    }

    fn subscription_cursor(
        lifecycle: CursorLifecycle,
        chunk_stream: SubscriptionCursorDataChunkStream,
        from_snapshot: bool,
    ) -> SubscriptionCursor {
        let state = if from_snapshot {
            SubscriptionCursorState::Fetch {
                from_snapshot: true,
                rw_timestamp: 1 << 16,
                expected_timestamp: None,
                init_query_timer: Instant::now(),
            }
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
        assert!(!response_stream.inner.cached_events.is_empty());

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
        assert!(!response_stream.inner.cached_events.is_empty());
        assert!(matches!(
            response_stream.subscription_state(),
            SubscriptionCursorState::InitLogStoreQuery { .. }
        ));

        response_stream.begin_fetch(&formats, session, false);
        let row = response_stream.next().await.unwrap().unwrap();
        assert_binary_i32(&row, 1);
        response_stream.commit_fetch();
        assert!(matches!(
            response_stream.subscription_state(),
            SubscriptionCursorState::Fetch {
                from_snapshot: false,
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
    async fn test_dropping_next_keeps_try_stream_await_state() {
        let started = Arc::new(AtomicBool::new(false));
        let (resume_tx, resume_rx) = oneshot::channel();
        let mut stream = QueryCursorDataChunkStream {
            inner: QueryCursorDataChunkStream::pending_for_test(started.clone(), resume_rx).boxed(),
        };

        assert!(stream.next().now_or_never().is_none());
        assert!(started.load(Ordering::Relaxed));
        resume_tx.send(()).unwrap();

        assert!(matches!(
            stream.next().await.unwrap().unwrap(),
            CursorDataChunkEvent::Barrier(CursorDataChunkBarrier::QueryEnd)
        ));
    }

    #[tokio::test]
    async fn test_session_shutdown_interrupts_fetch_and_drops_cursor_stream() {
        let session = Arc::new(SessionImpl::mock());
        let cursor_manager = session.get_cursor_manager();
        let lifecycle = CursorLifecycle::new(cursor_manager.session_shutdown_token());
        let query_shutdown_rx = lifecycle.query_shutdown_token();
        let (_chunk_tx, chunk_rx) = mpsc::channel(1);
        let fields = vec![Field::with_name(DataType::Int32, "v")];
        let query_stream = CursorQueryStream::local(
            tokio_stream::wrappers::ReceiverStream::new(chunk_rx),
            lifecycle.query_shutdown_sender(),
        );
        let cursor = QueryCursor {
            lifecycle,
            pg_response_stream: QueryCursorPgResponseStream::new(
                QueryCursorDataChunkStream::new(query_stream, fields.clone()),
                fields,
            ),
        };
        cursor_manager
            .add_query_cursor("cur".to_owned(), cursor)
            .await
            .unwrap();

        let fetch_manager = cursor_manager.clone();
        let fetch_args = handler_args(session);
        let fetch = tokio::spawn(async move {
            fetch_manager
                .get_rows_with_cursor(
                    "cur",
                    1,
                    fetch_args,
                    &vec![],
                    None,
                    &mut FetchCursorCancelHandle::new(),
                )
                .await
        });
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if cursor_manager.cursor_map.try_lock().is_err() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("FETCH must acquire the cursor map");

        cursor_manager.shutdown();
        let error = tokio::time::timeout(Duration::from_secs(1), fetch)
            .await
            .expect("session shutdown must wake FETCH")
            .unwrap()
            .expect_err("session shutdown must fail the active FETCH");
        assert!(error.to_string().contains("Session ended"));
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if cursor_manager.cursor_map.lock().await.is_empty() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("session shutdown must drop every cursor-owned stream");
        assert!(query_shutdown_rx.is_cancelled());
    }

    #[tokio::test]
    async fn test_query_cancel_timeout_and_format_changes_preserve_cursor_data() {
        let session = Arc::new(SessionImpl::mock());
        let args = handler_args(session.clone());
        let (lifecycle, chunk_stream, fields, _) = query_data_stream();
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
            let (lifecycle, chunk_stream, started) = subscription_data_stream(from_snapshot);
            let mut cursor = subscription_cursor(lifecycle, chunk_stream, from_snapshot);
            let cursor_metrics = Arc::new(CursorMetrics::new(&prometheus::Registry::new()));
            cursor.cursor_metrics = cursor_metrics.clone();
            let fetch_duration = cursor_metrics
                .subscription_cursor_fetch_duration
                .with_label_values(&[&cursor.subscription.name]);

            tokio::task::yield_now().await;
            assert!(
                !started.load(Ordering::Relaxed),
                "the high-level raw stream must remain demand-driven"
            );

            let mut cancelled_fetch = FetchCursorCancelHandle::new();
            assert!(cancelled_fetch.cancel_tx.cancel());
            let error = cursor
                .fetch(3, args.clone(), &vec![], None, &mut cancelled_fetch)
                .await
                .expect_err("CancelRequest must terminate only this FETCH");
            assert!(error.to_string().contains("Cancelled by user"));
            assert_eq!(fetch_duration.get_sample_count(), 0);
            session.clear_cancel_query_flag();
            tokio::task::yield_now().await;
            assert!(
                !started.load(Ordering::Relaxed),
                "a pre-cancelled FETCH must not advance the raw stream"
            );

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
            assert!(started.load(Ordering::Relaxed));
            assert_eq!(rows.len(), 1);
            assert_text_value(&rows[0], b"1");
            assert_eq!(fetch_duration.get_sample_count(), 1);

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
            assert_eq!(fetch_duration.get_sample_count(), 3);
        }
    }

    #[tokio::test]
    async fn test_cancelled_fetch_keeps_response_stream_cache() {
        let session = SessionImpl::mock();

        let (_query_lifecycle, stream, fields, _) = query_data_stream();
        assert_query_cancelled_fetch_keeps_response_stream_cache(
            QueryCursorPgResponseStream::new(stream, fields),
            &session,
            vec![Format::Binary],
        )
        .await;

        let (_subscription_lifecycle, stream, _) = subscription_data_stream(false);
        assert_subscription_cancelled_fetch_keeps_response_stream_cache(
            SubscriptionCursorPgResponseStream::new(
                stream,
                subscription_fields().get_output_fields(),
                SubscriptionCursorState::InitLogStoreQuery {
                    seek_timestamp: 1 << 16,
                    expected_timestamp: None,
                },
                Instant::now() + Duration::from_secs(60),
            ),
            &session,
            vec![Format::Binary, Format::Text, Format::Binary],
        )
        .await;
    }

    #[tokio::test]
    async fn test_subscription_idle_barrier_remains_nonblocking_across_fetches() {
        let (stream, _) =
            SubscriptionCursorDataChunkStream::for_test(vec![CursorDataChunkEvent::Barrier(
                CursorDataChunkBarrier::SubscriptionIdle,
            )]);
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
