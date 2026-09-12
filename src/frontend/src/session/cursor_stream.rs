// Copyright 2026 RisingWave Labs
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

//! Persistent raw cursor streams and PostgreSQL response adapters.

use std::collections::VecDeque;
use std::mem;
use std::pin::Pin;
use std::sync::{Arc, Weak};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use futures::stream::BoxStream;
use futures::{Stream, StreamExt};
use futures_async_stream::try_stream;
use pgwire::types::{Format, Row};
use risingwave_common::array::DataChunk;
use risingwave_common::catalog::{Field, TableId};
use risingwave_common::error::BoxedError;
use risingwave_common::row::{OwnedRow, Row as _, RowExt as _};

use super::{CursorQueryStream, FieldsManager, SubscriptionCursor, create_cursor_query_stream};
use crate::TableCatalog;
use crate::catalog::subscription_catalog::SubscriptionCatalog;
use crate::error::{ErrorCode, Result};
use crate::handler::HandlerArgs;
use crate::handler::query::gen_batch_plan_fragmenter;
use crate::handler::util::{StaticSessionData, to_pg_rows};
use crate::monitor::CursorMetrics;
use crate::scheduler::ReadSnapshot;
use crate::session::SessionImpl;
use crate::utils::WithOptions;

/// Schema and provenance of a raw cursor data chunk.
enum CursorDataChunkMetadata {
    /// A chunk from a regular query cursor.
    Query {
        /// Fields describing every column in the chunk.
        fields: Arc<Vec<Field>>,
    },
    /// A chunk from the initial snapshot or a log-store epoch of a subscription cursor.
    Subscription {
        /// Field mapping in effect when this chunk was produced.
        fields: Arc<FieldsManager>,
        /// Whether the chunk comes from the initial upstream-table snapshot.
        from_snapshot: bool,
        /// Snapshot or log-store epoch represented by the chunk.
        rw_timestamp: u64,
    },
}

/// Unformatted executor output and the metadata needed to interpret its rows.
pub(super) struct CursorDataChunk {
    chunk: DataChunk,
    metadata: CursorDataChunkMetadata,
}

impl CursorDataChunk {
    fn into_pg_rows(
        self,
        format: &CursorRowFormat,
    ) -> Result<(VecDeque<CursorPgRow>, CursorDataChunkMetadata)> {
        // Keep seek values typed and independent of the PostgreSQL result encoding.
        let mut seek_keys = match &self.metadata {
            CursorDataChunkMetadata::Query { .. } => None,
            CursorDataChunkMetadata::Subscription { fields, .. } => Some(
                self.chunk
                    .rows()
                    .map(|row| row.project(&fields.row_pk_indices).into_owned_row())
                    .collect::<VecDeque<_>>(),
            ),
        };
        let (column_types, formats) = match &self.metadata {
            CursorDataChunkMetadata::Query { fields } => (
                fields.iter().map(Field::data_type).collect::<Vec<_>>(),
                format.formats.clone(),
            ),
            CursorDataChunkMetadata::Subscription {
                fields,
                from_snapshot,
                ..
            } => {
                let (row_fields, mut row_formats) =
                    fields.get_row_stream_fields_and_formats(&format.formats, *from_snapshot)?;
                // Raw chunks omit the synthetic columns appended by build_row.
                row_formats.truncate(row_fields.len());
                (
                    row_fields.iter().map(Field::data_type).collect(),
                    row_formats,
                )
            }
        };
        let rows = to_pg_rows(&column_types, self.chunk, &formats, &format.session_data)?;
        // Each formatted subscription row must have one typed seek key from the same raw row.
        // Query cursor rows have no seek keys.
        debug_assert!(
            seek_keys
                .as_ref()
                .is_none_or(|keys| keys.len() == rows.len())
        );
        let rows = rows
            .into_iter()
            .map(|row| CursorPgRow {
                row,
                seek_pk_row: seek_keys.as_mut().map(|keys| {
                    keys.pop_front()
                        .expect("one seek key per formatted subscription row")
                }),
            })
            .collect();
        Ok((rows, self.metadata))
    }
}

/// An ordered, non-row event exposing a cursor's execution or data-availability transition.
///
/// Unlike [`std::task::Poll::Pending`], each barrier identifies a specific transition rather
/// than an arbitrary unfinished asynchronous operation. These events are not FETCH checkpoints.
pub(super) enum CursorDataChunkBarrier {
    /// The single query owned by a regular query cursor has completed.
    QueryEnd,
    /// A query for the subscription snapshot or a log-store epoch has started.
    /// Emitted before rows from that query.
    SubscriptionQueryStarted {
        /// Whether the query reads the initial upstream-table snapshot.
        from_snapshot: bool,
        /// Snapshot or log-store epoch read by the query.
        rw_timestamp: u64,
        /// Exact next log-store epoch, when known, used to detect a retention gap.
        expected_timestamp: Option<u64>,
        /// When query initialization began, for duration metrics and diagnostics.
        init_query_timer: Instant,
        /// Output schema for rows from this query.
        output_fields: Vec<Field>,
        /// Retention deadline associated with this query.
        expires_at: Instant,
    },
    /// A subscription query has completed; the stream will look for the next log-store epoch.
    SubscriptionNewEpoch {
        /// Lower bound for the next log-store epoch search.
        seek_timestamp: u64,
        /// Exact log-store epoch required at that position, when known, to detect a retention gap.
        expected_timestamp: Option<u64>,
    },
    /// No log-store epoch is currently available.
    /// Emitted before waiting for the next log-store epoch.
    /// This distinguishes an intentional idle wait from pending query startup or row reads.
    SubscriptionIdle,
    /// The wait for the next log-store epoch has finished.
    /// The stream will check for available log-store epochs again.
    SubscriptionIdleEnded,
    /// The output schema changed. Always end the current FETCH before consuming new-schema
    /// rows, even if it is empty and waiting. The new fields have already been published.
    SchemaChanged,
}

/// A raw data chunk or a control barrier, ordered by the cursor's persistent producer.
pub(super) enum CursorDataChunkEvent {
    Chunk(CursorDataChunk),
    Barrier(CursorDataChunkBarrier),
}

/// A persistent producer of raw query chunks followed by a query-completion barrier.
pub(super) struct QueryCursorDataChunkStream {
    /// Owns the query stream and its pending read across individual FETCH calls.
    inner: BoxStream<'static, std::result::Result<CursorDataChunkEvent, BoxedError>>,
}

impl QueryCursorDataChunkStream {
    pub(super) fn new(query_stream: CursorQueryStream, fields: Vec<Field>) -> Self {
        Self {
            inner: Self::event_stream(query_stream, fields).boxed(),
        }
    }

    #[try_stream(ok = CursorDataChunkEvent, error = BoxedError)]
    async fn event_stream(mut query_stream: CursorQueryStream, fields: Vec<Field>) {
        let fields = Arc::new(fields);
        while let Some(chunk) = query_stream.next().await {
            yield CursorDataChunkEvent::Chunk(CursorDataChunk {
                chunk: chunk?,
                metadata: CursorDataChunkMetadata::Query {
                    fields: fields.clone(),
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

/// A persistent subscription event stream, independent of any single FETCH future.
pub(super) struct SubscriptionCursorDataChunkStream {
    /// Owns the event producer, including its suspended asynchronous operations.
    inner: BoxStream<'static, std::result::Result<CursorDataChunkEvent, BoxedError>>,
}

impl SubscriptionCursorDataChunkStream {
    /// Starts a snapshot or log-store query with the snapshot selected before asynchronous startup.
    pub(super) async fn initiate_query(
        rw_timestamp: Option<u64>,
        dependent_table_id: TableId,
        handler_args: HandlerArgs,
        snapshot: ReadSnapshot,
    ) -> Result<(CursorQueryStream, Instant, Arc<TableCatalog>)> {
        let init_query_timer = Instant::now();
        let session = handler_args.session.clone();
        let table_catalog = session.get_table_by_id(dependent_table_id)?;
        let plan_result = SubscriptionCursor::init_batch_plan_for_subscription_cursor(
            rw_timestamp,
            dependent_table_id,
            handler_args,
            None,
        )?;
        let plan_fragmenter_result = gen_batch_plan_fragmenter(&session, plan_result)?;
        let (query_stream, _) =
            create_cursor_query_stream(session, plan_fragmenter_result, snapshot).await?;
        Ok((query_stream, init_query_timer, table_catalog))
    }

    /// FULL supplies `Fetch` with its query already started during DECLARE; SINCE supplies
    /// `InitLogStoreQuery`. Constructing this container does not poll either state.
    pub(super) fn new(
        subscription: Arc<SubscriptionCatalog>,
        dependent_table_id: TableId,
        handler_context: SubscriptionCursorHandlerContext,
        fields_manager: FieldsManager,
        state: SubscriptionCursorState<CursorQueryStream>,
        cursor_metrics: Arc<CursorMetrics>,
    ) -> Self {
        Self {
            inner: Self::event_stream(
                subscription,
                dependent_table_id,
                handler_context,
                fields_manager,
                state,
                cursor_metrics,
            )
            .boxed(),
        }
    }

    #[try_stream(ok = CursorDataChunkEvent, error = BoxedError)]
    async fn event_stream(
        subscription: Arc<SubscriptionCatalog>,
        dependent_table_id: TableId,
        handler_context: SubscriptionCursorHandlerContext,
        fields_manager: FieldsManager,
        mut state: SubscriptionCursorState<CursorQueryStream>,
        cursor_metrics: Arc<CursorMetrics>,
    ) {
        let mut fields_manager = Arc::new(fields_manager);
        loop {
            match mem::replace(&mut state, SubscriptionCursorState::Invalid) {
                SubscriptionCursorState::InitLogStoreQuery {
                    seek_timestamp,
                    expected_timestamp,
                } => {
                    let (rw_timestamp, next_expected_timestamp) =
                        match SubscriptionCursor::get_next_rw_timestamp(
                            seek_timestamp,
                            dependent_table_id,
                            expected_timestamp,
                            handler_context.handler_args()?,
                            &subscription,
                        )
                        .await?
                        {
                            (Some(rw_timestamp), next_expected_timestamp) => {
                                (rw_timestamp, next_expected_timestamp)
                            }
                            (None, _) => {
                                state = SubscriptionCursorState::InitLogStoreQuery {
                                    seek_timestamp,
                                    expected_timestamp,
                                };
                                yield CursorDataChunkEvent::Barrier(
                                    CursorDataChunkBarrier::SubscriptionIdle,
                                );
                                let session = handler_context.handler_args()?.session;
                                session
                                    .env()
                                    .hummock_snapshot_manager()
                                    .wait_table_change_log_notification(
                                        dependent_table_id,
                                        seek_timestamp,
                                    )
                                    .await?;
                                yield CursorDataChunkEvent::Barrier(
                                    CursorDataChunkBarrier::SubscriptionIdleEnded,
                                );
                                continue;
                            }
                        };
                    let handler_args = handler_context.handler_args()?;
                    let snapshot = ReadSnapshot::FrontendPinned {
                        snapshot: handler_args
                            .session
                            .env()
                            .hummock_snapshot_manager()
                            .acquire(),
                    };
                    let (query_stream, init_query_timer, catalog) = Self::initiate_query(
                        Some(rw_timestamp),
                        dependent_table_id,
                        handler_args,
                        snapshot,
                    )
                    .await?;
                    let schema_changed =
                        Arc::make_mut(&mut fields_manager).try_refill_fields(&catalog);
                    let expires_at =
                        Instant::now() + Duration::from_secs(subscription.retention_seconds);
                    state = SubscriptionCursorState::Fetch {
                        from_snapshot: false,
                        rw_timestamp,
                        query_stream,
                        expected_timestamp: next_expected_timestamp,
                        init_query_timer,
                    };
                    // Publish the new fields before ending FETCH so the next Parse/Describe
                    // sees them. The current FETCH keeps its original response descriptors.
                    yield CursorDataChunkEvent::Barrier(
                        CursorDataChunkBarrier::SubscriptionQueryStarted {
                            from_snapshot: false,
                            rw_timestamp,
                            expected_timestamp: next_expected_timestamp,
                            init_query_timer,
                            output_fields: fields_manager.get_output_fields(),
                            expires_at,
                        },
                    );
                    // Always stop before new-schema rows: even an empty waiting FETCH may
                    // already have sent an extended-protocol Describe for the old schema.
                    if schema_changed {
                        yield CursorDataChunkEvent::Barrier(CursorDataChunkBarrier::SchemaChanged);
                    }
                }
                SubscriptionCursorState::Fetch {
                    from_snapshot,
                    rw_timestamp,
                    mut query_stream,
                    expected_timestamp,
                    init_query_timer,
                } => match query_stream.next().await {
                    Some(Ok(chunk)) => {
                        state = SubscriptionCursorState::Fetch {
                            from_snapshot,
                            rw_timestamp,
                            query_stream,
                            expected_timestamp,
                            init_query_timer,
                        };
                        yield CursorDataChunkEvent::Chunk(CursorDataChunk {
                            chunk,
                            metadata: CursorDataChunkMetadata::Subscription {
                                fields: fields_manager.clone(),
                                from_snapshot,
                                rw_timestamp,
                            },
                        });
                    }
                    Some(Err(error)) => Err(error)?,
                    None => {
                        // EOF disarms the wrapper's unfinished-execution cancellation.
                        drop(query_stream);
                        cursor_metrics
                            .subscription_cursor_query_duration
                            .with_label_values(&[&subscription.name])
                            .observe(init_query_timer.elapsed().as_millis() as _);
                        let seek_timestamp = expected_timestamp.unwrap_or_else(|| rw_timestamp + 1);
                        state = SubscriptionCursorState::InitLogStoreQuery {
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
                SubscriptionCursorState::Invalid => return Ok(()),
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

/// Shared subscription state definition for the producer and its response adapter.
/// The producer owns a `CursorQueryStream` in Fetch; the adapter uses `()` and tracks position
/// through ordered barriers without duplicating query ownership. This is not a rollback checkpoint.
pub(super) enum SubscriptionCursorState<QueryStream = ()> {
    /// Looking for the next available log-store epoch.
    InitLogStoreQuery {
        /// Lower bound for the next log-store epoch search.
        seek_timestamp: u64,
        /// Exact next log-store epoch, when known, used to detect a retention gap.
        expected_timestamp: Option<u64>,
    },
    /// Reading the initial snapshot or a log-store epoch.
    Fetch {
        /// Whether the query reads the initial upstream-table snapshot.
        from_snapshot: bool,
        /// Snapshot or log-store epoch read by the query.
        rw_timestamp: u64,
        /// Exact next log-store epoch, when known, used to detect a retention gap.
        expected_timestamp: Option<u64>,
        /// When query initialization began, for duration metrics and diagnostics.
        init_query_timer: Instant,
        /// Holds the actual query stream in the producer. The response adapter does not use this
        /// field, so it uses the unit type `()` and assigns `()` instead.
        query_stream: QueryStream,
    },
    /// The producer reported an unrecoverable error.
    Invalid,
}

impl<QueryStream> SubscriptionCursorState<QueryStream> {
    /// Returns a copy of the cursor state with `query_stream` replaced by `()`.
    pub(super) fn strip_query_stream(&self) -> SubscriptionCursorState {
        match self {
            Self::InitLogStoreQuery {
                seek_timestamp,
                expected_timestamp,
            } => SubscriptionCursorState::InitLogStoreQuery {
                seek_timestamp: *seek_timestamp,
                expected_timestamp: *expected_timestamp,
            },
            Self::Fetch {
                from_snapshot,
                rw_timestamp,
                expected_timestamp,
                init_query_timer,
                ..
            } => SubscriptionCursorState::Fetch {
                from_snapshot: *from_snapshot,
                rw_timestamp: *rw_timestamp,
                expected_timestamp: *expected_timestamp,
                init_query_timer: *init_query_timer,
                query_stream: (),
            },
            Self::Invalid => SubscriptionCursorState::Invalid,
        }
    }
}

/// Stored planning context for later subscription queries, without a strong session reference.
/// Reconstructed handler arguments may still retain the session during an asynchronous operation.
pub(super) struct SubscriptionCursorHandlerContext {
    session: Weak<SessionImpl>,
    sql: Arc<str>,
    normalized_sql: String,
    with_options: WithOptions,
}

impl SubscriptionCursorHandlerContext {
    pub(super) fn new(handler_args: &HandlerArgs) -> Self {
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

/// PostgreSQL formats and session settings used for row conversion, without retaining a session.
struct CursorRowFormat {
    formats: Vec<Format>,
    session_data: StaticSessionData,
}

impl CursorRowFormat {
    fn new(formats: &[Format], session: &SessionImpl) -> Self {
        Self {
            formats: formats.to_vec(),
            session_data: StaticSessionData {
                timezone: session.config().timezone(),
            },
        }
    }
}

/// A formatted output row paired with its original typed subscription seek key, if any.
struct CursorPgRow {
    row: Row,
    seek_pk_row: Option<OwnedRow>,
}

/// Shared row conversion and buffering for query and subscription response streams.
/// Only unread formatted rows are retained; rows returned to FETCH cannot be replayed.
///
/// TODO: Add transactional buffering in a subsequent PR (Goal 3) so rows consumed by a cancelled
///     FETCH can be replayed.
struct CursorPgResponseStreamInner<S> {
    /// Released on terminal EOF or error, but retained across individual FETCH boundaries.
    data_stream: Option<S>,
    /// Records terminal failure without clearing it on subsequent EOF polls.
    /// Subscription invalidity checks use `SubscriptionCursorState::Invalid` instead.
    failed: bool,
    current_rows: VecDeque<CursorPgRow>,
    current_metadata: Option<Arc<CursorDataChunkMetadata>>,
    /// Fixed for each underlying query, matching the existing row-stream initialization.
    row_format: Option<Arc<CursorRowFormat>>,
    output_fields: Vec<Field>,
    /// A command boundary, not necessarily the end of the persistent data stream.
    fetch_stream_terminated: bool,
}

enum CursorPgResponsePollItem {
    Row {
        row: CursorPgRow,
        metadata: Arc<CursorDataChunkMetadata>,
    },
    Barrier(CursorDataChunkBarrier),
    DataChunkStreamEnd,
}

impl<S> CursorPgResponseStreamInner<S> {
    fn new(data_stream: S, output_fields: Vec<Field>) -> Self {
        Self {
            data_stream: Some(data_stream),
            failed: false,
            current_rows: VecDeque::new(),
            current_metadata: None,
            row_format: None,
            output_fields,
            fetch_stream_terminated: false,
        }
    }

    fn begin_fetch(&mut self, format: Arc<CursorRowFormat>) {
        self.row_format.get_or_insert(format);
        self.fetch_stream_terminated = false;
    }

    /// Releases the data stream at the end of its lifecycle. To end only the current FETCH,
    /// set `fetch_stream_terminated` to true; `begin_fetch` resets it to false to resume.
    fn mark_completed(&mut self, is_failed: bool) {
        self.failed = is_failed;
        self.fetch_stream_terminated = true;
        self.data_stream = None;
        self.current_rows.clear();
        self.current_metadata = None;
    }
}

impl<S> CursorPgResponseStreamInner<S>
where
    S: Stream<Item = std::result::Result<CursorDataChunkEvent, BoxedError>> + Unpin,
{
    fn poll_next_item(&mut self, cx: &mut Context<'_>) -> Poll<Result<CursorPgResponsePollItem>> {
        if self.data_stream.is_none() {
            return Poll::Ready(Ok(CursorPgResponsePollItem::DataChunkStreamEnd));
        }
        loop {
            if let Some(row) = self.current_rows.pop_front() {
                return Poll::Ready(Ok(CursorPgResponsePollItem::Row {
                    row,
                    metadata: self.current_metadata.as_ref().unwrap().clone(),
                }));
            }
            match self.data_stream.as_mut().unwrap().poll_next_unpin(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Some(Ok(CursorDataChunkEvent::Chunk(chunk)))) => {
                    let format = self
                        .row_format
                        .as_ref()
                        .expect("row formatting must be initialized before reading cursor chunks");
                    match chunk.into_pg_rows(format) {
                        Ok((rows, metadata)) => {
                            self.current_rows = rows;
                            self.current_metadata = Some(Arc::new(metadata));
                        }
                        Err(error) => {
                            self.mark_completed(true);
                            return Poll::Ready(Err(error));
                        }
                    }
                }
                Poll::Ready(Some(Ok(CursorDataChunkEvent::Barrier(barrier)))) => {
                    return Poll::Ready(Ok(CursorPgResponsePollItem::Barrier(barrier)));
                }
                Poll::Ready(Some(Err(error))) => {
                    self.mark_completed(true);
                    return Poll::Ready(Err(error.into()));
                }
                Poll::Ready(None) => {
                    self.mark_completed(false);
                    return Poll::Ready(Ok(CursorPgResponsePollItem::DataChunkStreamEnd));
                }
            }
        }
    }
}

/// A query response stream that retains unread rows across FETCH boundaries.
pub(super) struct QueryCursorPgResponseStream {
    inner: CursorPgResponseStreamInner<QueryCursorDataChunkStream>,
}

impl QueryCursorPgResponseStream {
    pub(super) fn new(data_stream: QueryCursorDataChunkStream, output_fields: Vec<Field>) -> Self {
        Self {
            inner: CursorPgResponseStreamInner::new(data_stream, output_fields),
        }
    }

    pub(super) fn fields(&self) -> Vec<Field> {
        self.inner.output_fields.clone()
    }

    pub(super) fn begin_fetch(&mut self, formats: &[Format], session: &SessionImpl) {
        self.inner
            .begin_fetch(Arc::new(CursorRowFormat::new(formats, session)));
    }
}

impl Stream for QueryCursorPgResponseStream {
    type Item = Result<Row>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.inner.fetch_stream_terminated {
            return Poll::Ready(None);
        }
        match this.inner.poll_next_item(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(error)) => Poll::Ready(Some(Err(error))),
            Poll::Ready(Ok(CursorPgResponsePollItem::Row { row, .. })) => {
                Poll::Ready(Some(Ok(row.row)))
            }
            Poll::Ready(Ok(CursorPgResponsePollItem::Barrier(
                CursorDataChunkBarrier::QueryEnd,
            ))) => {
                this.inner.mark_completed(false);
                Poll::Ready(None)
            }
            Poll::Ready(Ok(CursorPgResponsePollItem::DataChunkStreamEnd)) => Poll::Ready(None),
            Poll::Ready(Ok(CursorPgResponsePollItem::Barrier(_))) => {
                this.inner.mark_completed(true);
                Poll::Ready(Some(Err(ErrorCode::InternalError(
                    "query cursor received a subscription cursor barrier".to_owned(),
                )
                .into())))
            }
        }
    }
}

/// A subscription response stream that applies log-store epoch and schema barriers in order.
/// Position and seek metadata advance as rows/events are consumed, without FETCH rollback.
pub(super) struct SubscriptionCursorPgResponseStream {
    inner: CursorPgResponseStreamInner<SubscriptionCursorDataChunkStream>,
    subscription_state: SubscriptionCursorState,
    seek_pk_row: Option<OwnedRow>,
    expires_at: Instant,
    is_idle: bool,
    /// Whether an idle FETCH should wait when it has yielded no rows. Once rows have been
    /// yielded, idle always ends the FETCH, regardless of this flag.
    should_wait_when_idle_and_empty: bool,
    yielded_rows: usize,
    /// Used for synthetic subscription columns; raw columns keep the query's row format.
    fetch_format: Option<Arc<CursorRowFormat>>,
}

impl SubscriptionCursorPgResponseStream {
    pub(super) fn new(
        data_stream: SubscriptionCursorDataChunkStream,
        output_fields: Vec<Field>,
        subscription_state: SubscriptionCursorState,
        expires_at: Instant,
    ) -> Self {
        Self {
            inner: CursorPgResponseStreamInner::new(data_stream, output_fields),
            subscription_state,
            seek_pk_row: None,
            expires_at,
            is_idle: false,
            should_wait_when_idle_and_empty: false,
            yielded_rows: 0,
            fetch_format: None,
        }
    }

    pub(super) fn fields(&self) -> Vec<Field> {
        self.inner.output_fields.clone()
    }

    pub(super) fn state_info_string(&self) -> String {
        match self.subscription_state() {
            SubscriptionCursorState::InitLogStoreQuery {
                seek_timestamp,
                expected_timestamp,
            } => format!(
                "InitLogStoreQuery {{ seek_timestamp: {}, expected_timestamp: {:?} }}",
                seek_timestamp, expected_timestamp
            ),
            SubscriptionCursorState::Fetch {
                from_snapshot,
                rw_timestamp,
                expected_timestamp,
                init_query_timer,
                ..
            } => format!(
                "Fetch {{ from_snapshot: {}, rw_timestamp: {}, expected_timestamp: {:?}, cached rows: {}, query init at {}ms before }}",
                from_snapshot,
                rw_timestamp,
                expected_timestamp,
                self.inner.current_rows.len(),
                init_query_timer.elapsed().as_millis()
            ),
            SubscriptionCursorState::Invalid => "Invalid".to_owned(),
        }
    }

    pub(super) fn subscription_state(&self) -> &SubscriptionCursorState {
        &self.subscription_state
    }

    pub(super) fn seek_pk_row(&self) -> Option<OwnedRow> {
        self.seek_pk_row.clone()
    }

    pub(super) fn is_expired(&self, now: Instant) -> bool {
        now > self.expires_at
    }

    pub(super) fn begin_fetch(
        &mut self,
        formats: &[Format],
        session: &SessionImpl,
        should_wait_when_idle_and_empty: bool,
    ) {
        let format = Arc::new(CursorRowFormat::new(formats, session));
        self.inner.begin_fetch(format.clone());
        self.fetch_format = Some(format);
        self.should_wait_when_idle_and_empty = should_wait_when_idle_and_empty;
        self.yielded_rows = 0;
    }

    fn project_row(
        &mut self,
        row: Row,
        seek_pk_row: Option<OwnedRow>,
        metadata: &CursorDataChunkMetadata,
    ) -> Result<Row> {
        let CursorDataChunkMetadata::Subscription {
            fields,
            from_snapshot,
            rw_timestamp,
        } = metadata
        else {
            return Err(ErrorCode::InternalError(
                "subscription cursor received query metadata".to_owned(),
            )
            .into());
        };
        let format = self.fetch_format.as_ref().unwrap();
        let (_, row_formats) =
            fields.get_row_stream_fields_and_formats(&format.formats, *from_snapshot)?;
        let mut row = SubscriptionCursor::build_row(
            row.take(),
            (!from_snapshot).then_some(*rw_timestamp),
            &row_formats,
            &format.session_data,
        )?;
        let row = row.project(&fields.row_output_col_indices);
        self.seek_pk_row = seek_pk_row;
        Ok(row)
    }
}

impl Stream for SubscriptionCursorPgResponseStream {
    type Item = Result<Row>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        const INVALID_CURSOR_ERROR_MESSAGE: &str =
            "Subscription cursor data stream ended unexpectedly; close and recreate the cursor";
        let this = self.get_mut();
        if let SubscriptionCursorState::Invalid = this.subscription_state {
            return Poll::Ready(Some(Err(ErrorCode::InternalError(
                INVALID_CURSOR_ERROR_MESSAGE.to_owned(),
            )
            .into())));
        }
        loop {
            if this.inner.fetch_stream_terminated {
                return Poll::Ready(None);
            }
            match this.inner.poll_next_item(cx) {
                Poll::Pending => {
                    if this.is_idle
                        && (this.yielded_rows > 0 || !this.should_wait_when_idle_and_empty)
                    {
                        this.inner.fetch_stream_terminated = true;
                        return Poll::Ready(None);
                    }
                    return Poll::Pending;
                }
                Poll::Ready(Err(error)) => {
                    this.subscription_state = SubscriptionCursorState::Invalid;
                    return Poll::Ready(Some(Err(error)));
                }
                Poll::Ready(Ok(CursorPgResponsePollItem::Row { row, metadata })) => {
                    let row = this.project_row(row.row, row.seek_pk_row, &metadata);
                    if row.is_err() {
                        this.inner.mark_completed(true);
                        this.subscription_state = SubscriptionCursorState::Invalid;
                    } else {
                        this.is_idle = false;
                        this.yielded_rows += 1;
                    }
                    return Poll::Ready(Some(row));
                }
                Poll::Ready(Ok(CursorPgResponsePollItem::Barrier(barrier))) => {
                    this.is_idle = false;
                    match barrier {
                        CursorDataChunkBarrier::SubscriptionQueryStarted {
                            from_snapshot,
                            rw_timestamp,
                            expected_timestamp,
                            init_query_timer,
                            output_fields,
                            expires_at,
                        } => {
                            this.subscription_state = SubscriptionCursorState::Fetch {
                                query_stream: (),
                                from_snapshot,
                                rw_timestamp,
                                expected_timestamp,
                                init_query_timer,
                            };
                            this.inner.output_fields = output_fields;
                            this.expires_at = expires_at;
                            this.inner.row_format = this.fetch_format.clone();
                        }
                        CursorDataChunkBarrier::SubscriptionNewEpoch {
                            seek_timestamp,
                            expected_timestamp,
                        } => {
                            this.subscription_state = SubscriptionCursorState::InitLogStoreQuery {
                                seek_timestamp,
                                expected_timestamp,
                            };
                        }
                        CursorDataChunkBarrier::SubscriptionIdle => {
                            this.is_idle = true;
                            if this.yielded_rows > 0 || !this.should_wait_when_idle_and_empty {
                                this.inner.fetch_stream_terminated = true;
                                return Poll::Ready(None);
                            }
                        }
                        CursorDataChunkBarrier::SubscriptionIdleEnded => {}
                        CursorDataChunkBarrier::SchemaChanged => {
                            // Old-schema rows have been drained. Retain the producer and end the
                            // current FETCH regardless of waiting mode, so that any new-schema rows
                            // will be left for the next FETCH with latest description.
                            debug_assert!(this.inner.current_rows.is_empty());
                            this.inner.current_metadata = None;
                            // No new-query rows have been formatted yet. The next FETCH supplies
                            // formats for its new schema, which may have a different column count,
                            // so the current format should not be used any more.
                            this.inner.row_format = None;
                            this.inner.fetch_stream_terminated = true;
                            return Poll::Ready(None);
                        }
                        CursorDataChunkBarrier::QueryEnd => {
                            this.inner.mark_completed(true);
                            this.subscription_state = SubscriptionCursorState::Invalid;
                            return Poll::Ready(Some(Err(ErrorCode::InternalError(
                                "subscription cursor received a query cursor barrier".to_owned(),
                            )
                            .into())));
                        }
                    }
                }
                // Subscription cursor query stream expects never terminated, it will periodically
                // launch new query to query new log-store epoch when consumed one.
                Poll::Ready(Ok(CursorPgResponsePollItem::DataChunkStreamEnd)) => {
                    this.inner.mark_completed(true);
                    this.subscription_state = SubscriptionCursorState::Invalid;
                    return Poll::Ready(Some(Err(ErrorCode::InternalError(
                        INVALID_CURSOR_ERROR_MESSAGE.to_owned(),
                    )
                    .into())));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use pgwire::pg_server::Session as _;
    use risingwave_batch::task::ShutdownToken;
    use risingwave_common::array::DataChunkTestExt;
    use risingwave_common::types::DataType;
    use risingwave_sqlparser::parser::Parser;
    use tokio::sync::{mpsc, oneshot};
    use tokio_stream::wrappers::ReceiverStream;

    use super::super::{CursorShutdownHandle, FetchCursorCancelHandle};
    use super::*;
    use crate::handler::fetch_cursor::handle_parse;
    use crate::handler::util::to_pg_field;

    /// Verifies that the cursor data chunk stream preserves chunks and schema, then emits
    /// `QueryEnd` and EOF; this does not exercise a real executor.
    #[tokio::test]
    async fn test_local_cursor_query_data_chunk_stream_preserves_chunks() {
        let (shutdown_tx, shutdown_rx) = ShutdownToken::new();
        let (chunk_tx, chunk_rx) = mpsc::channel(2);
        let chunks = [
            DataChunk::from_pretty("i\n1\n2"),
            DataChunk::from_pretty("i\n3"),
        ];
        for chunk in &chunks {
            chunk_tx.try_send(Ok(chunk.clone())).unwrap();
        }
        drop(chunk_tx);
        let fields = vec![Field::with_name(DataType::Int32, "v")];
        let mut stream = QueryCursorDataChunkStream::new(
            CursorQueryStream::local(ReceiverStream::new(chunk_rx), shutdown_tx),
            fields.clone(),
        );

        for expected in chunks {
            let CursorDataChunkEvent::Chunk(chunk) = stream.next().await.unwrap().unwrap() else {
                panic!("expected a query data chunk, but received a barrier");
            };
            assert_eq!(chunk.chunk, expected);
            let CursorDataChunkMetadata::Query { fields: actual } = chunk.metadata else {
                panic!("query chunks must carry query metadata");
            };
            assert_eq!(*actual, fields);
        }
        assert!(matches!(
            stream.next().await.unwrap().unwrap(),
            CursorDataChunkEvent::Barrier(CursorDataChunkBarrier::QueryEnd)
        ));
        assert!(stream.next().await.is_none());
        drop(stream);
        assert!(!shutdown_rx.is_cancelled());
    }

    /// Verifies that dropping a pending poll leaves the channel-backed cursor query owned by the
    /// cursor data chunk stream, which can still receive a chunk; this does not exercise a real
    /// executor.
    #[tokio::test]
    async fn test_pending_cursor_data_chunk_event_poll_preserves_local_cursor_query_ownership() {
        let (shutdown_tx, shutdown_rx) = ShutdownToken::new();
        let (chunk_tx, chunk_rx) = mpsc::channel(1);
        let mut stream = QueryCursorDataChunkStream::new(
            CursorQueryStream::local(ReceiverStream::new(chunk_rx), shutdown_tx),
            vec![Field::with_name(DataType::Int32, "v")],
        );

        let mut next = Box::pin(stream.next());
        assert!(futures::poll!(next.as_mut()).is_pending());
        drop(next);
        assert!(!shutdown_rx.is_cancelled());
        assert!(!chunk_tx.is_closed());

        let expected = DataChunk::from_pretty("i\n1");
        chunk_tx.try_send(Ok(expected.clone())).unwrap();
        let event = tokio::time::timeout(Duration::from_secs(1), stream.next())
            .await
            .expect("the same producer must still receive query output")
            .unwrap()
            .unwrap();
        let CursorDataChunkEvent::Chunk(chunk) = event else {
            panic!("the pending query must resume with its chunk");
        };
        assert_eq!(chunk.chunk, expected);
        assert!(!shutdown_rx.is_cancelled());
    }

    /// A test-controlled producer: the gate simulates a log-store epoch wait, not a real
    /// snapshot-manager notification. The counter detects re-entry before the suspended wait.
    #[try_stream(ok = CursorDataChunkEvent, error = BoxedError)]
    async fn gated_log_store_epoch_events(
        wait_starts: Arc<AtomicUsize>,
        resume_rx: oneshot::Receiver<()>,
    ) {
        yield CursorDataChunkEvent::Barrier(CursorDataChunkBarrier::SubscriptionIdle);
        wait_starts.fetch_add(1, Ordering::Relaxed);
        resume_rx.await.unwrap();
        yield CursorDataChunkEvent::Barrier(CursorDataChunkBarrier::SubscriptionIdleEnded);
    }

    /// Verifies that dropping temporary polls preserves a subscription cursor data chunk stream
    /// coroutine's pending operation and resumes it once the gate opens, without restarting the
    /// simulated log-store epoch wait; this does not exercise a real executor.
    #[tokio::test]
    async fn test_subscription_cursor_data_chunk_stream_preserves_pending_operation() {
        let wait_starts = Arc::new(AtomicUsize::new(0));
        let (resume_tx, resume_rx) = oneshot::channel();
        let mut stream = SubscriptionCursorDataChunkStream {
            inner: gated_log_store_epoch_events(wait_starts.clone(), resume_rx).boxed(),
        };
        assert!(matches!(
            stream.next().await.unwrap().unwrap(),
            CursorDataChunkEvent::Barrier(CursorDataChunkBarrier::SubscriptionIdle)
        ));

        for _ in 0..2 {
            let mut next = Box::pin(stream.next());
            assert!(futures::poll!(next.as_mut()).is_pending());
            drop(next);
            assert_eq!(wait_starts.load(Ordering::Relaxed), 1);
        }
        resume_tx.send(()).unwrap();
        let event = tokio::time::timeout(Duration::from_secs(1), stream.next())
            .await
            .expect("the suspended operation must resume when its gate opens")
            .unwrap()
            .unwrap();
        assert!(matches!(
            event,
            CursorDataChunkEvent::Barrier(CursorDataChunkBarrier::SubscriptionIdleEnded)
        ));
        assert_eq!(wait_starts.load(Ordering::Relaxed), 1);
        assert!(stream.next().await.is_none());
    }

    /// Creates a subscription cursor data chunk stream for a FULL subscription cursor, starting in
    /// `SubscriptionCursorState::Fetch` with the supplied query stream. This bypasses real snapshot
    /// selection and query startup. The handler context contains an empty weak session reference,
    /// so attempting to look up the next log-store epoch returns an error.
    fn full_subscription_cursor_data_chunk_stream_with_empty_weak_session_ref_for_test(
        query_stream: CursorQueryStream,
    ) -> SubscriptionCursorDataChunkStream {
        SubscriptionCursorDataChunkStream::new(
            Arc::new(SubscriptionCatalog {
                name: "subscription".to_owned(),
                retention_seconds: 60,
                ..Default::default()
            }),
            TableId::new(1),
            SubscriptionCursorHandlerContext {
                session: Weak::new(),
                sql: "".into(),
                normalized_sql: String::new(),
                with_options: WithOptions::default(),
            },
            subscription_fields_for_test("v").as_ref().clone(),
            SubscriptionCursorState::Fetch {
                from_snapshot: true,
                rw_timestamp: 12,
                expected_timestamp: Some(20),
                init_query_timer: Instant::now(),
                query_stream,
            },
            Arc::new(CursorMetrics::for_test()),
        )
    }

    /// Verifies that a subscription cursor data chunk stream retains its pending channel-backed
    /// query, yields snapshot chunks, then advances to the expected log-store epoch without
    /// cancellation. This does not exercise query startup, real snapshot selection, or log-store
    /// notifications.
    #[tokio::test]
    async fn test_subscription_cursor_data_chunk_stream_preserves_pending_query_and_advances_epoch()
    {
        let (shutdown_tx, shutdown_rx) = ShutdownToken::new();
        let (chunk_tx, chunk_rx) = mpsc::channel(2);
        let mut stream =
            full_subscription_cursor_data_chunk_stream_with_empty_weak_session_ref_for_test(
                CursorQueryStream::local(ReceiverStream::new(chunk_rx), shutdown_tx),
            );
        for _ in 0..2 {
            let mut next = Box::pin(stream.next());
            assert!(futures::poll!(next.as_mut()).is_pending());
            drop(next);
            assert!(!chunk_tx.is_closed());
            assert!(!shutdown_rx.is_cancelled());
        }
        let expected = DataChunk::from_pretty("i i\n7 42");
        chunk_tx.try_send(Ok(expected.clone())).unwrap();
        chunk_tx.try_send(Ok(expected.clone())).unwrap();
        drop(chunk_tx);
        let mut previous_fields = None;
        for _ in 0..2 {
            let CursorDataChunkEvent::Chunk(chunk) = stream.next().await.unwrap().unwrap() else {
                panic!("expected the existing snapshot query's chunk");
            };
            assert_eq!(chunk.chunk, expected);
            let CursorDataChunkMetadata::Subscription {
                fields,
                from_snapshot,
                rw_timestamp,
            } = chunk.metadata
            else {
                panic!("expected subscription metadata");
            };
            assert!(from_snapshot);
            assert_eq!(rw_timestamp, 12);
            if let Some(previous) = previous_fields {
                assert!(Arc::ptr_eq(&previous, &fields));
            }
            previous_fields = Some(fields);
        }
        assert!(matches!(
            stream.next().await.unwrap().unwrap(),
            CursorDataChunkEvent::Barrier(CursorDataChunkBarrier::SubscriptionNewEpoch {
                seek_timestamp: 20,
                expected_timestamp: Some(20),
            })
        ));
        assert!(!shutdown_rx.is_cancelled());
        // Only the next poll attempts a lookup; this fixture deliberately has no live session.
        let error = stream.next().await.unwrap().err().unwrap();
        assert!(error.to_string().contains("session ended"));
        assert!(!shutdown_rx.is_cancelled());
    }

    /// Verifies that an injected query error terminates the subscription cursor data chunk stream
    /// and requests cancellation of its failed query. The channel-backed input is also dropped;
    /// token signaling is not proof of real executor termination.
    #[tokio::test]
    async fn test_subscription_cursor_data_chunk_stream_cancels_failed_query() {
        let (shutdown_tx, shutdown_rx) = ShutdownToken::new();
        let (chunk_tx, chunk_rx) = mpsc::channel(1);
        let mut stream =
            full_subscription_cursor_data_chunk_stream_with_empty_weak_session_ref_for_test(
                CursorQueryStream::local(ReceiverStream::new(chunk_rx), shutdown_tx),
            );
        chunk_tx
            .try_send(Err(anyhow::anyhow!("injected query failure").into()))
            .unwrap();
        let error = stream.next().await.unwrap().err().unwrap();
        assert!(error.to_string().contains("injected query failure"));
        assert!(stream.next().await.is_none());
        assert!(chunk_tx.is_closed());
        assert!(shutdown_rx.is_cancelled());
    }

    fn assert_text_row(row: &Row, expected: &[Option<&str>]) {
        let actual = row
            .values()
            .iter()
            .map(|value| value.as_deref())
            .collect::<Vec<_>>();
        let expected = expected
            .iter()
            .map(|value| value.map(str::as_bytes))
            .collect::<Vec<_>>();
        assert_eq!(actual, expected);
    }

    /// A query cursor's local query output channel without an executor.
    fn pending_query_response_stream_for_test() -> (
        QueryCursorPgResponseStream,
        mpsc::Sender<std::result::Result<DataChunk, BoxedError>>,
    ) {
        let (shutdown_tx, _shutdown_rx) = ShutdownToken::new();
        let (chunk_tx, chunk_rx) = mpsc::channel(4);
        let fields = vec![Field::with_name(DataType::Int32, "v")];
        let data_stream = QueryCursorDataChunkStream::new(
            CursorQueryStream::local(ReceiverStream::new(chunk_rx), shutdown_tx),
            fields.clone(),
        );
        (
            QueryCursorPgResponseStream::new(data_stream, fields),
            chunk_tx,
        )
    }

    /// Places a hidden key before the visible value; snapshot chunks omit the synthetic columns.
    fn subscription_fields_for_test(value_name: &str) -> Arc<FieldsManager> {
        Arc::new(FieldsManager {
            columns_catalog: vec![],
            row_fields: vec![
                Field::with_name(DataType::Int32, "hidden_pk"),
                Field::with_name(DataType::Int32, value_name),
                Field::with_name(DataType::Varchar, "op"),
                Field::with_name(DataType::Int64, "rw_timestamp"),
            ],
            row_output_col_indices: vec![1, 2, 3],
            row_pk_indices: vec![0],
            stream_chunk_row_indices: vec![0, 1],
            op_index: 2,
        })
    }

    /// Injects events directly without subscription planning, execution, or notifications.
    fn pending_subscription_response_stream_for_test(
        fields: &FieldsManager,
        expires_at: Instant,
    ) -> (
        SubscriptionCursorPgResponseStream,
        mpsc::Sender<std::result::Result<CursorDataChunkEvent, BoxedError>>,
    ) {
        let (event_tx, event_rx) = mpsc::channel(8);
        let stream = SubscriptionCursorPgResponseStream::new(
            SubscriptionCursorDataChunkStream {
                inner: ReceiverStream::new(event_rx).boxed(),
            },
            fields.get_output_fields(),
            SubscriptionCursorState::InitLogStoreQuery {
                seek_timestamp: 0,
                expected_timestamp: None,
            },
            expires_at,
        );
        (stream, event_tx)
    }

    fn subscription_chunk_for_test(
        fields: Arc<FieldsManager>,
        from_snapshot: bool,
        rw_timestamp: u64,
        chunk: &str,
    ) -> CursorDataChunkEvent {
        CursorDataChunkEvent::Chunk(CursorDataChunk {
            chunk: DataChunk::from_pretty(chunk),
            metadata: CursorDataChunkMetadata::Subscription {
                fields,
                from_snapshot,
                rw_timestamp,
            },
        })
    }

    /// Verifies that the query response stream retains unread rows across `begin_fetch` calls
    /// and ends after consuming its chunks; this does not execute SQL FETCH or a real executor.
    #[tokio::test]
    async fn test_query_cursor_pg_response_stream_preserves_remaining_rows() {
        let session = SessionImpl::mock();
        let (mut stream, chunk_tx) = pending_query_response_stream_for_test();
        for chunk in ["i\n1\n2", "i\n3"] {
            chunk_tx
                .try_send(Ok(DataChunk::from_pretty(chunk)))
                .unwrap();
        }
        drop(chunk_tx);

        stream.begin_fetch(&[], &session);
        assert_text_row(&stream.next().await.unwrap().unwrap(), &[Some("1")]);
        stream.begin_fetch(&[], &session);
        for expected in ["2", "3"] {
            assert_text_row(&stream.next().await.unwrap().unwrap(), &[Some(expected)]);
        }
        assert!(stream.next().await.is_none());
        stream.begin_fetch(&[], &session);
        assert!(stream.next().await.is_none());
    }

    /// Verifies that the subscription response stream projects snapshot and log-store epoch rows
    /// and keeps the hidden key as seek metadata; this does not exercise a real executor.
    #[tokio::test]
    async fn test_subscription_cursor_pg_response_stream_projects_rows_and_seek_key() {
        let session = SessionImpl::mock();
        let fields = subscription_fields_for_test("v");
        let rw_timestamp = crate::handler::util::convert_unix_millis_to_logstore_u64(1700000000000);
        for from_snapshot in [true, false] {
            let (mut stream, event_tx) =
                pending_subscription_response_stream_for_test(&fields, Instant::now());
            event_tx
                .try_send(Ok(CursorDataChunkEvent::Barrier(
                    CursorDataChunkBarrier::SubscriptionQueryStarted {
                        from_snapshot,
                        rw_timestamp,
                        expected_timestamp: None,
                        init_query_timer: Instant::now(),
                        output_fields: fields.get_output_fields(),
                        expires_at: Instant::now() + Duration::from_secs(60),
                    },
                )))
                .unwrap();
            let chunk = if from_snapshot {
                "i i\n7 42"
            } else {
                "i i T\n7 42 Delete"
            };
            event_tx
                .try_send(Ok(subscription_chunk_for_test(
                    fields.clone(),
                    from_snapshot,
                    rw_timestamp,
                    chunk,
                )))
                .unwrap();

            stream.begin_fetch(&[], &session, false);
            let row = stream.next().await.unwrap().unwrap();
            let expected = if from_snapshot {
                [Some("42"), Some("Insert"), None]
            } else {
                [Some("42"), Some("Delete"), Some("1700000000000")]
            };
            assert_text_row(&row, &expected);
            assert_eq!(
                stream.seek_pk_row().unwrap(),
                OwnedRow::new(vec![Some(7i32.into())])
            );
        }
    }

    /// Verifies zero, single, and per-column format codes for snapshot and log-store output.
    /// The fixture has a hidden key, so output-format positions differ from raw-column positions.
    /// Events are injected directly; protocol coverage lives in the extended-mode E2E suite.
    #[tokio::test]
    async fn test_subscription_cursor_pg_response_stream_honors_result_format_codes() {
        let session = SessionImpl::mock();
        let fields = subscription_fields_for_test("v");
        let timestamp = 1700000000000i64;
        let rw_timestamp =
            crate::handler::util::convert_unix_millis_to_logstore_u64(timestamp as u64);
        for from_snapshot in [true, false] {
            for (formats, binary_value, binary_timestamp) in [
                (vec![], false, false),
                (vec![Format::Binary], true, true),
                (
                    vec![Format::Binary, Format::Text, Format::Text],
                    true,
                    false,
                ),
                (
                    vec![Format::Text, Format::Binary, Format::Binary],
                    false,
                    true,
                ),
            ] {
                let (mut stream, event_tx) =
                    pending_subscription_response_stream_for_test(&fields, Instant::now());
                let chunk = if from_snapshot {
                    "i i\n7 42"
                } else {
                    "i i T\n7 42 Delete"
                };
                event_tx
                    .try_send(Ok(subscription_chunk_for_test(
                        fields.clone(),
                        from_snapshot,
                        rw_timestamp,
                        chunk,
                    )))
                    .unwrap();
                stream.begin_fetch(&formats, &session, false);
                let row = stream.next().await.unwrap().unwrap();
                let value = if binary_value {
                    42i32.to_be_bytes().to_vec()
                } else {
                    b"42".to_vec()
                };
                let timestamp = if from_snapshot {
                    None
                } else if binary_timestamp {
                    Some(timestamp.to_be_bytes().to_vec())
                } else {
                    Some(timestamp.to_string().into_bytes())
                };
                assert_eq!(row.values().len(), 3);
                assert_eq!(row.values()[0].as_deref(), Some(value.as_slice()));
                assert_eq!(
                    row.values()[1].as_deref(),
                    Some(if from_snapshot {
                        b"Insert".as_slice()
                    } else {
                        b"Delete".as_slice()
                    })
                );
                assert_eq!(row.values()[2].as_deref(), timestamp.as_deref());
                assert_eq!(
                    stream.seek_pk_row(),
                    Some(OwnedRow::new(vec![Some(7i32.into())]))
                );
            }
        }
    }

    /// Verifies that binary-formatted subscription rows retain typed seek keys in declared primary
    /// key order, advancing per yielded row rather than per chunk. This uses injected snapshot
    /// events.
    #[tokio::test]
    async fn test_subscription_cursor_pg_response_stream_preserves_typed_seek_keys_with_binary_rows()
     {
        use risingwave_common::catalog::{ColumnCatalog, ColumnDesc, ColumnId};
        use risingwave_common::util::sort_util::{ColumnOrder, OrderType};

        let session = SessionImpl::mock();
        let catalog = TableCatalog {
            columns: vec![
                ColumnCatalog::visible(ColumnDesc::named("a", ColumnId::new(1), DataType::Int32)),
                ColumnCatalog::visible(ColumnDesc::named("b", ColumnId::new(2), DataType::Int32)),
            ],
            pk: vec![
                ColumnOrder::new(1, OrderType::ascending()),
                ColumnOrder::new(0, OrderType::ascending()),
            ],
            ..Default::default()
        };
        let fields = Arc::new(FieldsManager::new(&catalog));
        let (mut stream, event_tx) =
            pending_subscription_response_stream_for_test(&fields, Instant::now());
        event_tx
            .try_send(Ok(subscription_chunk_for_test(
                fields,
                true,
                12,
                "i i\n7 42\n8 43",
            )))
            .unwrap();
        stream.begin_fetch(&[Format::Binary], &session, false);
        assert!(stream.seek_pk_row().is_none());
        let row = stream.next().await.unwrap().unwrap();
        assert_eq!(
            row.values()[0].as_deref(),
            Some(7i32.to_be_bytes().as_slice())
        );
        assert_eq!(
            stream.seek_pk_row(),
            Some(OwnedRow::new(vec![Some(42i32.into()), Some(7i32.into())]))
        );
        assert_eq!(stream.inner.current_rows.len(), 1);
        stream.begin_fetch(&[Format::Binary], &session, false);
        let row = stream.next().await.unwrap().unwrap();
        assert_eq!(
            row.values()[0].as_deref(),
            Some(8i32.to_be_bytes().as_slice())
        );
        assert_eq!(
            stream.seek_pk_row(),
            Some(OwnedRow::new(vec![Some(43i32.into()), Some(8i32.into())]))
        );
    }

    /// Verifies that every schema boundary ends the subscription response stream's current FETCH,
    /// with new fields, position, and expiry already visible but new-schema rows left unread.
    /// This uses injected events, not SQL FETCH or a real executor.
    #[tokio::test]
    async fn test_subscription_cursor_pg_response_stream_handles_log_store_epoch_and_schema_boundaries()
     {
        let session = SessionImpl::mock();
        let old_fields = subscription_fields_for_test("old_value");
        let new_fields = subscription_fields_for_test("new_value");
        let mut latest_fields = subscription_fields_for_test("latest_value");
        // The second schema change changes the visible column's type as well as its name.
        Arc::get_mut(&mut latest_fields).unwrap().row_fields[1] =
            Field::with_name(DataType::Varchar, "latest_value");
        let initial_expiry = Instant::now();
        let renewed_expiry = initial_expiry + Duration::from_secs(60);
        let latest_expiry = renewed_expiry + Duration::from_secs(60);
        for should_wait_when_idle_and_empty in [false, true] {
            let (mut stream, event_tx) =
                pending_subscription_response_stream_for_test(&old_fields, initial_expiry);
            stream.begin_fetch(&[], &session, should_wait_when_idle_and_empty);
            event_tx
                .try_send(Ok(CursorDataChunkEvent::Barrier(
                    CursorDataChunkBarrier::SubscriptionNewEpoch {
                        seek_timestamp: 12,
                        expected_timestamp: Some(12),
                    },
                )))
                .unwrap();
            // The response stream consumes the position barrier, but has no row to yield yet.
            assert!(futures::poll!(stream.next()).is_pending());
            assert!(matches!(
                stream.subscription_state(),
                SubscriptionCursorState::InitLogStoreQuery {
                    seek_timestamp: 12,
                    expected_timestamp: Some(12),
                }
            ));
            assert!(stream.is_expired(initial_expiry + Duration::from_secs(1)));

            for event in [
                CursorDataChunkEvent::Barrier(CursorDataChunkBarrier::SubscriptionQueryStarted {
                    from_snapshot: false,
                    rw_timestamp: 12,
                    expected_timestamp: Some(20),
                    init_query_timer: initial_expiry,
                    output_fields: new_fields.get_output_fields(),
                    expires_at: renewed_expiry,
                }),
                CursorDataChunkEvent::Barrier(CursorDataChunkBarrier::SchemaChanged),
                subscription_chunk_for_test(
                    new_fields.clone(),
                    false,
                    12,
                    "i i T\n8 99 Insert\n9 100 Insert",
                ),
                CursorDataChunkEvent::Barrier(CursorDataChunkBarrier::SubscriptionNewEpoch {
                    seek_timestamp: 20,
                    expected_timestamp: Some(20),
                }),
                CursorDataChunkEvent::Barrier(CursorDataChunkBarrier::SubscriptionQueryStarted {
                    from_snapshot: false,
                    rw_timestamp: 20,
                    expected_timestamp: None,
                    init_query_timer: initial_expiry,
                    output_fields: latest_fields.get_output_fields(),
                    expires_at: latest_expiry,
                }),
                CursorDataChunkEvent::Barrier(CursorDataChunkBarrier::SchemaChanged),
                subscription_chunk_for_test(
                    latest_fields.clone(),
                    false,
                    20,
                    "i T T\n10 changed Insert",
                ),
            ] {
                event_tx.try_send(Ok(event)).unwrap();
            }

            // Even an empty waiting FETCH stops. Publish the next query's metadata before
            // the boundary so the next Parse/Describe sees the schema of its unread rows.
            assert!(stream.next().await.is_none());
            assert!(stream.inner.current_rows.is_empty());
            assert!(stream.inner.current_metadata.is_none());
            assert_eq!(stream.fields(), new_fields.get_output_fields());
            assert!(!stream.is_expired(initial_expiry + Duration::from_secs(1)));
            assert!(stream.is_expired(renewed_expiry + Duration::from_secs(1)));
            assert!(matches!(
                stream.subscription_state(),
                SubscriptionCursorState::Fetch {
                    rw_timestamp: 12,
                    expected_timestamp: Some(20),
                    ..
                }
            ));
            stream.begin_fetch(&[], &session, should_wait_when_idle_and_empty);
            let row = stream.next().await.unwrap().unwrap();
            assert_eq!(row.values()[0].as_deref(), Some(b"99".as_slice()));
            assert_eq!(stream.fields(), new_fields.get_output_fields());
            assert_eq!(stream.inner.current_rows.len(), 1);
            assert!(stream.inner.current_metadata.is_some());
            assert!(matches!(
                stream.subscription_state(),
                SubscriptionCursorState::Fetch {
                    from_snapshot: false,
                    rw_timestamp: 12,
                    expected_timestamp: Some(20),
                    ..
                }
            ));
            assert!(!stream.is_expired(initial_expiry + Duration::from_secs(1)));
            assert!(stream.is_expired(renewed_expiry + Duration::from_secs(1)));

            // Drain the buffered row before reading the second schema barrier; do not discard it.
            let row = stream.next().await.unwrap().unwrap();
            assert_eq!(row.values()[0].as_deref(), Some(b"100".as_slice()));
            // The second boundary also ends FETCH after its two old-schema rows, with the
            // latest metadata published but no rows from that query consumed.
            assert!(stream.next().await.is_none());
            assert!(stream.inner.current_rows.is_empty());
            assert!(stream.inner.current_metadata.is_none());
            assert!(!event_tx.is_closed());
            assert_eq!(stream.fields(), latest_fields.get_output_fields());
            assert!(matches!(
                stream.subscription_state(),
                SubscriptionCursorState::Fetch {
                    rw_timestamp: 20,
                    expected_timestamp: None,
                    ..
                }
            ));
            assert!(!stream.is_expired(renewed_expiry + Duration::from_secs(1)));
            assert!(stream.is_expired(latest_expiry + Duration::from_secs(1)));

            // The next query's first chunk remains unread until the next FETCH begins.
            stream.begin_fetch(&[], &session, should_wait_when_idle_and_empty);
            let row = stream.next().await.unwrap().unwrap();
            assert_eq!(row.values()[0].as_deref(), Some(b"changed".as_slice()));
            assert_eq!(stream.fields(), latest_fields.get_output_fields());
            assert!(matches!(
                stream.subscription_state(),
                SubscriptionCursorState::Fetch {
                    from_snapshot: false,
                    rw_timestamp: 20,
                    expected_timestamp: None,
                    ..
                }
            ));
            assert!(!stream.is_expired(renewed_expiry + Duration::from_secs(1)));
            assert!(stream.is_expired(latest_expiry + Duration::from_secs(1)));
        }
    }

    /// Verifies a subscription FETCH retains its response fields while the next Parse/Describe
    /// sees the new schema, for empty and nonempty FETCH commands with or without waiting.
    /// This uses injected events, not SQL FETCH or a real executor.
    #[tokio::test]
    async fn test_subscription_cursor_fetch_preserves_response_fields_across_schema_change() {
        let old_fields = subscription_fields_for_test("v");
        let mut new_fields = old_fields.as_ref().clone();
        // Simulate adding a visible column between the old value and the synthetic columns.
        new_fields
            .row_fields
            .insert(2, Field::with_name(DataType::Varchar, "added_value"));
        new_fields.row_output_col_indices = vec![1, 2, 3, 4];
        new_fields.stream_chunk_row_indices = vec![0, 1, 2];
        new_fields.op_index = 3;
        let new_fields = Arc::new(new_fields);
        let expected_old_desc = old_fields
            .get_output_fields()
            .iter()
            .map(to_pg_field)
            .collect::<Vec<_>>();
        let expected_new_desc = new_fields
            .get_output_fields()
            .iter()
            .map(to_pg_field)
            .collect::<Vec<_>>();

        // SubscriptionCursor::fetch enables idle waiting only for a positive timeout:
        // None is nonwaiting; Some(5) allows an empty FETCH to wait. SchemaChanged must
        // end both immediately, without waiting for the timeout or consuming new-schema rows.
        for timeout_seconds in [None, Some(5)] {
            // false reaches the boundary with no rows; true queues an old-schema row first,
            // so FETCH must return that accumulated row with its original descriptors.
            for has_old_row in [false, true] {
                let session = Arc::new(SessionImpl::mock());
                let manager = session.get_cursor_manager();
                let expires_at = Instant::now() + Duration::from_secs(60);
                let (pg_response_stream, event_tx) =
                    pending_subscription_response_stream_for_test(&old_fields, expires_at);
                manager
                    .add_subscription_cursor(SubscriptionCursor {
                        shutdown_handle: CursorShutdownHandle::new(),
                        cursor_name: "cursor".to_owned(),
                        subscription: Arc::new(SubscriptionCatalog {
                            name: "subscription".to_owned(),
                            retention_seconds: 60,
                            ..Default::default()
                        }),
                        dependent_table_id: TableId::new(1),
                        pg_response_stream,
                        cursor_metrics: session.env().cursor_metrics.clone(),
                        last_fetch: Instant::now(),
                    })
                    .await
                    .unwrap();
                if has_old_row {
                    event_tx
                        .try_send(Ok(subscription_chunk_for_test(
                            old_fields.clone(),
                            true,
                            0,
                            "i i\n7 42",
                        )))
                        .unwrap();
                }
                for event in [
                    CursorDataChunkEvent::Barrier(
                        CursorDataChunkBarrier::SubscriptionQueryStarted {
                            from_snapshot: false,
                            rw_timestamp: 12,
                            expected_timestamp: None,
                            init_query_timer: Instant::now(),
                            output_fields: new_fields.get_output_fields(),
                            expires_at,
                        },
                    ),
                    CursorDataChunkEvent::Barrier(CursorDataChunkBarrier::SchemaChanged),
                    subscription_chunk_for_test(
                        new_fields.clone(),
                        false,
                        12,
                        "i i T T\n8 99 added Insert",
                    ),
                ] {
                    event_tx.try_send(Ok(event)).unwrap();
                }

                let sql = "fetch 10 from cursor";
                let stmt = Parser::parse_sql(sql).unwrap().pop().unwrap();
                let args = HandlerArgs::new(session.clone(), &stmt, sql.into()).unwrap();
                let prepared = handle_parse(args.clone(), stmt.clone(), vec![])
                    .await
                    .unwrap();
                let (_, described_fields) = session.clone().describe_statement(prepared).unwrap();
                assert_eq!(described_fields, expected_old_desc);

                // Explicit per-column codes must be selected again for the new column count.
                let (rows, response_fields) = manager
                    .get_rows_with_cursor(
                        "cursor",
                        10,
                        args.clone(),
                        &vec![Format::Text; described_fields.len()],
                        timeout_seconds,
                        &mut FetchCursorCancelHandle::new(),
                    )
                    .await
                    .unwrap();
                assert_eq!(rows.len(), usize::from(has_old_row));
                if has_old_row {
                    assert_text_row(&rows[0], &[Some("42"), Some("Insert"), None]);
                }
                // Simple mode uses these response fields; extended mode already described them.
                assert_eq!(response_fields, described_fields);
                assert!(!event_tx.is_closed());

                let prepared = handle_parse(args.clone(), stmt, vec![]).await.unwrap();
                let (_, described_fields) = session.clone().describe_statement(prepared).unwrap();
                assert_eq!(described_fields, expected_new_desc);
                let (rows, response_fields) = manager
                    .get_rows_with_cursor(
                        "cursor",
                        1,
                        args,
                        &vec![Format::Text; described_fields.len()],
                        timeout_seconds,
                        &mut FetchCursorCancelHandle::new(),
                    )
                    .await
                    .unwrap();
                assert_eq!(rows.len(), 1);
                assert_eq!(response_fields, described_fields);
                assert_eq!(rows[0].values().len(), response_fields.len());
                assert_eq!(rows[0].values()[1].as_deref(), Some(b"added".as_slice()));
            }
        }
    }

    /// Verifies that subscription idle boundaries end non-waiting reads or reads that already
    /// yielded rows, while other pending operations remain pending; this uses injected events.
    #[tokio::test]
    async fn test_subscription_cursor_pg_response_stream_distinguishes_idle_from_pending() {
        let session = SessionImpl::mock();
        let fields = subscription_fields_for_test("v");
        let (mut stream, event_tx) =
            pending_subscription_response_stream_for_test(&fields, Instant::now());
        stream.begin_fetch(&[], &session, false);
        assert!(futures::poll!(stream.next()).is_pending());
        event_tx
            .try_send(Ok(CursorDataChunkEvent::Barrier(
                CursorDataChunkBarrier::SubscriptionIdle,
            )))
            .unwrap();
        assert!(stream.next().await.is_none());

        stream.begin_fetch(&[], &session, true);
        assert!(futures::poll!(stream.next()).is_pending());
        event_tx
            .try_send(Ok(CursorDataChunkEvent::Barrier(
                CursorDataChunkBarrier::SubscriptionIdleEnded,
            )))
            .unwrap();
        stream.begin_fetch(&[], &session, false);
        assert!(futures::poll!(stream.next()).is_pending());

        for event in [
            CursorDataChunkEvent::Barrier(CursorDataChunkBarrier::SubscriptionQueryStarted {
                from_snapshot: true,
                rw_timestamp: 0,
                expected_timestamp: None,
                init_query_timer: Instant::now(),
                output_fields: fields.get_output_fields(),
                expires_at: Instant::now() + Duration::from_secs(60),
            }),
            subscription_chunk_for_test(fields, true, 0, "i i\n7 42"),
            CursorDataChunkEvent::Barrier(CursorDataChunkBarrier::SubscriptionNewEpoch {
                seek_timestamp: 1,
                expected_timestamp: None,
            }),
            CursorDataChunkEvent::Barrier(CursorDataChunkBarrier::SubscriptionIdle),
        ] {
            event_tx.try_send(Ok(event)).unwrap();
        }
        stream.begin_fetch(&[], &session, true);
        assert_text_row(
            &stream.next().await.unwrap().unwrap(),
            &[Some("42"), Some("Insert"), None],
        );
        assert!(stream.next().await.is_none());
    }

    /// Verifies that a row-conversion error terminates the query response stream and releases its
    /// channel-backed input, without returning subsequent chunks; this does not exercise a real executor.
    #[tokio::test]
    async fn test_query_cursor_pg_response_stream_rejects_invalid_formats() {
        let session = SessionImpl::mock();
        let (mut stream, chunk_tx) = pending_query_response_stream_for_test();
        chunk_tx
            .try_send(Ok(DataChunk::from_pretty("i\n1")))
            .unwrap();
        chunk_tx
            .try_send(Ok(DataChunk::from_pretty("i\n2")))
            .unwrap();
        stream.begin_fetch(&[Format::Text, Format::Text], &session);
        let error = stream.next().await.unwrap().unwrap_err();
        assert!(error.to_string().contains("format codes length"));
        assert!(chunk_tx.is_closed());
        assert!(stream.next().await.is_none());
        stream.begin_fetch(&[], &session);
        assert!(stream.next().await.is_none());
    }

    /// Verifies that a source error or unexpected EOF invalidates the subscription response
    /// stream and every subsequent poll returns an error; this uses injected events, not a real executor.
    #[tokio::test]
    async fn test_subscription_cursor_pg_response_stream_invalidates_on_stream_failure() {
        let session = SessionImpl::mock();
        let fields = subscription_fields_for_test("v");
        for source_error in [false, true] {
            let (mut stream, event_tx) =
                pending_subscription_response_stream_for_test(&fields, Instant::now());
            if source_error {
                event_tx
                    .try_send(Err(anyhow::anyhow!("injected source error").into()))
                    .unwrap();
            }
            drop(event_tx);
            stream.begin_fetch(&[], &session, true);
            let error = stream.next().await.unwrap().unwrap_err();
            let expected = if source_error {
                "injected source error"
            } else {
                "ended unexpectedly"
            };
            assert!(error.to_string().contains(expected));
            assert!(stream.inner.failed);
            assert!(matches!(
                stream.subscription_state(),
                SubscriptionCursorState::Invalid
            ));
            // Once invalid, every poll returns an error, even before another FETCH begins.
            let error = stream.next().await.unwrap().unwrap_err();
            assert!(error.to_string().contains("ended unexpectedly"));
            stream.begin_fetch(&[], &session, true);
            let error = stream.next().await.unwrap().unwrap_err();
            assert!(error.to_string().contains("ended unexpectedly"));
            let error = stream.next().await.unwrap().unwrap_err();
            assert!(error.to_string().contains("ended unexpectedly"));
        }
    }
}
