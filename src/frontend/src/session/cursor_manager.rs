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

use core::mem;
use core::time::Duration;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};
use std::time::Instant;

use anyhow::anyhow;
use bytes::Bytes;
use futures::{Stream, StreamExt};
use itertools::Itertools;
use parking_lot::Mutex;
use pgwire::pg_field_descriptor::PgFieldDescriptor;
use pgwire::pg_response::StatementType;
use pgwire::types::{Format, FormatIterator, Row};
use risingwave_batch::task::{ShutdownSender, ShutdownToken};
use risingwave_common::array::DataChunk;
use risingwave_common::catalog::{ColumnCatalog, Field};
use risingwave_common::error::BoxedError;
use risingwave_common::row::OwnedRow;
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
    BatchPlanFragmenterResult, RwBatchQueryPlanResult, distribute_execute_for_cursor,
    local_execute_for_cursor,
};
use crate::handler::util::{
    StaticSessionData, convert_logstore_u64_to_unix_millis, pg_value_format, to_pg_field,
};
use crate::monitor::{CursorMetrics, PeriodicCursorMetrics};
use crate::optimizer::PlanRoot;
use crate::optimizer::plan_node::{BatchFilter, BatchLogSeqScan, BatchSeqScan, generic};
use crate::optimizer::property::{Order, RequiredDist};
use crate::scheduler::{
    DistributedQueryStream, LocalQueryStream, QueryManager, ReadSnapshot, SchedulerError,
};
use crate::utils::Condition;
use crate::{OptimizerContext, OptimizerContextRef, TableCatalog};

#[path = "cursor_stream.rs"]
mod cursor_stream;
use cursor_stream::{
    QueryCursorDataChunkStream, QueryCursorPgResponseStream, SubscriptionCursorDataChunkStream,
    SubscriptionCursorHandlerContext, SubscriptionCursorPgResponseStream, SubscriptionCursorState,
};

/// Creates raw cursor-owned output using a snapshot selected by the caller before startup.
/// The snapshot is retained across asynchronous fragmentation and scheduling, without consulting
/// the current transaction's snapshot again when a suspended startup resumes.
async fn create_cursor_query_stream(
    session: Arc<SessionImpl>,
    plan_fragmenter_result: BatchPlanFragmenterResult,
    snapshot: ReadSnapshot,
) -> Result<(CursorQueryStream, Vec<Field>)> {
    let BatchPlanFragmenterResult {
        plan_fragmenter,
        query_mode,
        schema,
        ..
    } = plan_fragmenter_result;

    let query = plan_fragmenter.generate_complete_query().await?;
    tracing::trace!("Generated query after plan fragmenter: {:?}", &query);

    // Cursor-owned queries outlive individual statements and must not inherit statement_timeout.
    let stream = match query_mode {
        QueryMode::Auto => unreachable!(),
        QueryMode::Local => {
            let (shutdown_tx, shutdown_rx) = ShutdownToken::new();
            CursorQueryStream::local(
                local_execute_for_cursor(session, query, shutdown_rx, snapshot)?,
                shutdown_tx,
            )
        }
        QueryMode::Distributed => CursorQueryStream::distributed(
            distribute_execute_for_cursor(session.clone(), query, snapshot).await?,
            session.env().query_manager().clone(),
        ),
    };
    Ok((stream, schema.fields))
}

/// Cursor-scoped shutdown resources, separate from individual FETCH cancellation.
struct CursorShutdownHandle {
    /// Signals termination of this cursor's execution.
    shutdown_tx: ShutdownSender,
    /// Observes termination of this cursor's execution.
    shutdown_rx: ShutdownToken,
}

impl CursorShutdownHandle {
    fn new() -> Self {
        let (shutdown_tx, shutdown_rx) = ShutdownToken::new();
        Self {
            shutdown_tx,
            shutdown_rx,
        }
    }

    /// Returns a token for observing cursor shutdown in FETCH or local query execution.
    fn shutdown_token(&self) -> ShutdownToken {
        self.shutdown_rx.clone()
    }

    /// Returns a sender that the cursor manager can retain to request cursor shutdown.
    fn shutdown_sender(&self) -> ShutdownSender {
        self.shutdown_tx.clone()
    }

    fn shutdown(&self) {
        self.shutdown_tx.cancel();
    }
}

impl Drop for CursorShutdownHandle {
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// A cursor-owned query stream and the resources needed to stop its execution and clean up.
enum CursorQueryStreamInner {
    Local {
        stream: LocalQueryStream,
        shutdown_tx: ShutdownSender,
    },
    Distributed {
        stream: DistributedQueryStream,
        query_manager: QueryManager,
    },
}

/// Owns one cursor query's raw output and cancels unfinished execution when dropped.
///
/// Ownership begins after query scheduling returns a stream. The distributed registration guard
/// covers cancellation during scheduling, before this wrapper can take ownership.
pub struct CursorQueryStream {
    inner: CursorQueryStreamInner,
    /// Whether the underlying stream has reached EOF and no longer needs cancellation.
    finished: bool,
}

impl CursorQueryStream {
    /// Takes ownership of a local query stream and its executor's shutdown sender.
    pub(crate) fn local(stream: LocalQueryStream, shutdown_tx: ShutdownSender) -> Self {
        Self {
            inner: CursorQueryStreamInner::Local {
                stream,
                shutdown_tx,
            },
            finished: false,
        }
    }

    /// Takes ownership of a distributed query stream and cancels it by ID if dropped before EOF.
    pub(crate) fn distributed(stream: DistributedQueryStream, query_manager: QueryManager) -> Self {
        Self {
            inner: CursorQueryStreamInner::Distributed {
                stream,
                query_manager,
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
                stream,
                query_manager,
            } => {
                // Request cancellation before dropping the inner stream removes its registration.
                query_manager.cancel_queries_by_ids(
                    std::slice::from_ref(stream.query_id()),
                    "cursor closed",
                );
            }
        }
    }
}

pub struct FetchCursorCancelHandle {
    cancel_tx: ShutdownSender,
    cancel_rx: ShutdownToken,
}

impl FetchCursorCancelHandle {
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

pub enum Cursor {
    Subscription(SubscriptionCursor),
    Query(QueryCursor),
}
impl Cursor {
    fn shutdown_handle(&self) -> &CursorShutdownHandle {
        match self {
            Cursor::Subscription(cursor) => &cursor.shutdown_handle,
            Cursor::Query(cursor) => &cursor.shutdown_handle,
        }
    }

    pub async fn fetch(
        &mut self,
        count: u32,
        handler_args: HandlerArgs,
        formats: &Vec<Format>,
        timeout_seconds: Option<u64>,
        cancel_handle: &mut FetchCursorCancelHandle,
    ) -> Result<(Vec<Row>, Vec<PgFieldDescriptor>)> {
        let mut shutdown_rx = self.shutdown_handle().shutdown_token();
        // Dropping FETCH's future is safe for terminal shutdown because the cursor is discarded.
        tokio::select! {
            biased;
            _ = shutdown_rx.cancelled() => {
                Err(SchedulerError::QueryCancelled("cursor closed".to_owned()).into())
            }
            result = async {
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
            } => result,
        }
    }

    pub fn get_fields(&mut self) -> Vec<Field> {
        match self {
            Cursor::Subscription(cursor) => cursor.pg_response_stream.fields(),
            Cursor::Query(cursor) => cursor.pg_response_stream.fields(),
        }
    }
}

/// Polls a temporary borrow of the persistent response stream for one FETCH.
/// Cancellation can discard accumulated rows; transactional replay belongs to Goal 3.
async fn fetch_rows<S: Stream<Item = Result<Row>> + Unpin>(
    stream: &mut S,
    count: u32,
    timeout_seconds: Option<u64>,
    cancel_handle: &mut FetchCursorCancelHandle,
    mut record_poll: impl FnMut(Duration),
) -> Result<Vec<Row>> {
    let deadline =
        timeout_seconds.map(|seconds| tokio::time::Instant::now() + Duration::from_secs(seconds));
    let timeout = async {
        match deadline {
            Some(deadline) => tokio::time::sleep_until(deadline).await,
            None => std::future::pending::<()>().await,
        }
    };
    tokio::pin!(timeout);
    let mut rows = Vec::with_capacity(count.min(100) as usize);
    while rows.len() < count as usize {
        let started = Instant::now();
        let row = if timeout_seconds == Some(0) {
            // Keep the legacy zero-timeout behavior: allow an immediately-ready row, but do
            // not wait for pending work. Cancellation always takes priority over consuming data.
            tokio::select! {
                biased;
                _ = cancel_handle.cancelled() => {
                    return Err(SchedulerError::QueryCancelled("Cancelled by user".to_owned()).into());
                }
                row = stream.next() => row,
                _ = &mut timeout => break,
            }
        } else {
            tokio::select! {
                biased;
                _ = cancel_handle.cancelled() => {
                    return Err(SchedulerError::QueryCancelled("Cancelled by user".to_owned()).into());
                }
                _ = &mut timeout => break,
                row = stream.next() => row,
            }
        };
        let Some(row) = row.transpose()? else {
            break;
        };
        record_poll(started.elapsed());
        rows.push(row);
        // Ready rows may keep this task running without letting the timer driver advance.
        // Check elapsed time directly as well; zero timeout still returns at most one ready row.
        if deadline.is_some_and(|deadline| tokio::time::Instant::now() >= deadline) {
            break;
        }
    }
    Ok(rows)
}

pub struct QueryCursor {
    shutdown_handle: CursorShutdownHandle,
    pg_response_stream: QueryCursorPgResponseStream,
}

impl QueryCursor {
    /// Creates shutdown resources, selects the snapshot, and starts the cursor-owned query.
    pub(crate) async fn new(
        session: Arc<SessionImpl>,
        plan_fragmenter_result: BatchPlanFragmenterResult,
    ) -> Result<Self> {
        let shutdown_handle = CursorShutdownHandle::new();
        let snapshot = session.pinned_snapshot();
        let (query_stream, fields) =
            create_cursor_query_stream(session, plan_fragmenter_result, snapshot).await?;
        let data_stream = QueryCursorDataChunkStream::new(query_stream, fields.clone());
        Ok(Self {
            shutdown_handle,
            pg_response_stream: QueryCursorPgResponseStream::new(data_stream, fields),
        })
    }

    pub async fn fetch(
        &mut self,
        count: u32,
        formats: &Vec<Format>,
        handler_args: HandlerArgs,
        timeout_seconds: Option<u64>,
        cancel_handle: &mut FetchCursorCancelHandle,
    ) -> Result<(Vec<Row>, Vec<PgFieldDescriptor>)> {
        self.pg_response_stream
            .begin_fetch(formats, &handler_args.session);
        let rows = fetch_rows(
            &mut self.pg_response_stream,
            count,
            timeout_seconds,
            cancel_handle,
            |_| {},
        )
        .await?;
        let desc = self
            .pg_response_stream
            .fields()
            .iter()
            .map(to_pg_field)
            .collect();
        Ok((rows, desc))
    }
}

#[derive(Clone)]
struct FieldsManager {
    columns_catalog: Vec<ColumnCatalog>,
    // All row fields, including hidden pk, op, rw_timestamp and all non-hidden columns in the upstream table.
    row_fields: Vec<Field>,
    // Row output column indices based on `row_fields`.
    row_output_col_indices: Vec<usize>,
    // Row pk indices based on `row_fields`.
    row_pk_indices: Vec<usize>,
    // Stream chunk row indices based on `row_fields`.
    stream_chunk_row_indices: Vec<usize>,
    // The op index based on `row_fields`.
    op_index: usize,
}

impl FieldsManager {
    // pub const OP_FIELD: Field = Field::with_name(DataType::Varchar, "op".to_owned());
    // pub const RW_TIMESTAMP_FIELD: Field = Field::with_name(DataType::Int64, "rw_timestamp".to_owned());

    pub fn new(catalog: &TableCatalog) -> Self {
        let mut row_fields = Vec::new();
        let mut row_output_col_indices = Vec::new();
        let mut row_pk_indices = vec![0; catalog.pk.len()];
        let mut stream_chunk_row_indices = Vec::new();
        let mut output_idx = 0_usize;
        let pk_positions: HashMap<usize, usize> = catalog
            .pk
            .iter()
            .enumerate()
            .map(|(position, col_order)| (col_order.column_index, position))
            .collect();

        for (index, v) in catalog.columns.iter().enumerate() {
            if let Some(&pk_position) = pk_positions.get(&index) {
                // Seek predicates pair values with catalog.pk, not table-column order.
                row_pk_indices[pk_position] = output_idx;
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
            row_pk_indices,
            stream_chunk_row_indices,
            op_index,
        }
    }

    pub fn try_refill_fields(&mut self, catalog: &TableCatalog) -> bool {
        if self.columns_catalog.ne(&catalog.columns) {
            *self = Self::new(catalog);
            true
        } else {
            false
        }
    }

    pub fn get_output_fields(&self) -> Vec<Field> {
        self.row_output_col_indices
            .iter()
            .map(|&idx| self.row_fields[idx].clone())
            .collect()
    }

    /// Maps FETCH result formats to the full row layout, including hidden and synthetic columns.
    /// An empty list selects default text; a single code is expanded to all visible columns.
    pub fn get_row_stream_fields_and_formats(
        &self,
        formats: &[Format],
        from_snapshot: bool,
    ) -> Result<(Vec<Field>, Vec<Format>)> {
        let raw_indices = self
            .stream_chunk_row_indices
            .iter()
            .copied()
            .chain((!from_snapshot).then_some(self.op_index));
        let fields = raw_indices
            .map(|index| self.row_fields[index].clone())
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

pub struct SubscriptionCursor {
    shutdown_handle: CursorShutdownHandle,
    cursor_name: String,
    subscription: Arc<SubscriptionCatalog>,
    dependent_table_id: TableId,
    pg_response_stream: SubscriptionCursorPgResponseStream,
    cursor_metrics: Arc<CursorMetrics>,
    last_fetch: Instant,
}

impl SubscriptionCursor {
    pub async fn new(
        cursor_name: String,
        start_timestamp: Option<u64>,
        subscription: Arc<SubscriptionCatalog>,
        dependent_table_id: TableId,
        handler_args: &HandlerArgs,
        cursor_metrics: Arc<CursorMetrics>,
    ) -> Result<Self> {
        let shutdown_handle = CursorShutdownHandle::new();
        let (state, fields_manager) = if let Some(start_timestamp) = start_timestamp {
            let table_catalog = handler_args.session.get_table_by_id(dependent_table_id)?;
            (
                SubscriptionCursorState::InitLogStoreQuery {
                    seek_timestamp: start_timestamp,
                    expected_timestamp: None,
                },
                FieldsManager::new(&table_catalog),
            )
        } else {
            // FULL selects its snapshot and epoch during DECLARE, before startup can suspend.
            let snapshot = handler_args.session.pinned_snapshot();
            let pinned_epoch = match &snapshot {
                ReadSnapshot::FrontendPinned { snapshot } => {
                    snapshot
                        .version()
                        .state_table_info
                        .info()
                        .get(&dependent_table_id)
                        .ok_or_else(|| {
                            anyhow!("dependent_table_id {dependent_table_id} not exists")
                        })?
                        .committed_epoch
                }
                ReadSnapshot::Other(_) => {
                    return Err(ErrorCode::InternalError("Fetch Cursor can't start from specified query epoch. May run `set query_epoch = 0;`".to_owned()).into());
                }
                ReadSnapshot::ReadUncommitted => {
                    return Err(ErrorCode::InternalError(
                        "Fetch Cursor don't support read uncommitted".to_owned(),
                    )
                    .into());
                }
            };
            let (query_stream, init_query_timer, table_catalog) =
                SubscriptionCursorDataChunkStream::initiate_query(
                    None,
                    dependent_table_id,
                    handler_args.clone(),
                    snapshot,
                )
                .await?;
            (
                SubscriptionCursorState::Fetch {
                    from_snapshot: true,
                    rw_timestamp: pinned_epoch,
                    query_stream,
                    expected_timestamp: None,
                    init_query_timer,
                },
                FieldsManager::new(&table_catalog),
            )
        };
        let expires_at = Instant::now() + Duration::from_secs(subscription.retention_seconds);
        let output_fields = fields_manager.get_output_fields();
        let response_state = state.strip_query_stream();
        let data_stream = SubscriptionCursorDataChunkStream::new(
            subscription.clone(),
            dependent_table_id,
            SubscriptionCursorHandlerContext::new(handler_args),
            fields_manager,
            state,
            cursor_metrics.clone(),
        );
        Ok(Self {
            shutdown_handle,
            cursor_name,
            subscription,
            dependent_table_id,
            pg_response_stream: SubscriptionCursorPgResponseStream::new(
                data_stream,
                output_fields,
                response_state,
                expires_at,
            ),
            cursor_metrics,
            last_fetch: Instant::now(),
        })
    }

    pub async fn fetch(
        &mut self,
        count: u32,
        handler_args: HandlerArgs,
        formats: &Vec<Format>,
        timeout_seconds: Option<u64>,
        cancel_handle: &mut FetchCursorCancelHandle,
    ) -> Result<(Vec<Row>, Vec<PgFieldDescriptor>)> {
        if self.pg_response_stream.is_expired(Instant::now()) {
            return Err(ErrorCode::InternalError(
                "The cursor has exceeded its maximum lifetime, please recreate it (close then declare cursor).".to_owned(),
            ).into());
        }
        // A schema boundary publishes fields for the next FETCH before ending this one.
        // Keep this response's descriptors consistent with its rows and any earlier Describe.
        let desc = self
            .pg_response_stream
            .fields()
            .iter()
            .map(to_pg_field)
            .collect();
        self.pg_response_stream.begin_fetch(
            formats,
            &handler_args.session,
            timeout_seconds.is_some_and(|seconds| seconds > 0),
        );
        let metrics = &self.cursor_metrics;
        let subscription_name = &self.subscription.name;
        let rows = fetch_rows(
            &mut self.pg_response_stream,
            count,
            timeout_seconds,
            cancel_handle,
            |elapsed| {
                metrics
                    .subscription_cursor_fetch_duration
                    .with_label_values(&[subscription_name])
                    .observe(elapsed.as_millis() as _);
            },
        )
        .await?;
        self.last_fetch = Instant::now();
        Ok((rows, desc))
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

    pub fn gen_batch_plan_result(
        &self,
        handler_args: HandlerArgs,
    ) -> Result<RwBatchQueryPlanResult> {
        match self.pg_response_stream.subscription_state() {
            // Only used to return generated plans, so rw_timestamp are meaningless
            SubscriptionCursorState::InitLogStoreQuery { .. } => {
                Self::init_batch_plan_for_subscription_cursor(
                    Some(0),
                    self.dependent_table_id,
                    handler_args,
                    self.pg_response_stream.seek_pk_row(),
                )
            }
            SubscriptionCursorState::Fetch {
                from_snapshot,
                rw_timestamp,
                ..
            } => {
                if *from_snapshot {
                    Self::init_batch_plan_for_subscription_cursor(
                        None,
                        self.dependent_table_id,
                        handler_args,
                        self.pg_response_stream.seek_pk_row(),
                    )
                } else {
                    Self::init_batch_plan_for_subscription_cursor(
                        Some(*rw_timestamp),
                        self.dependent_table_id,
                        handler_args,
                        self.pg_response_stream.seek_pk_row(),
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
        seek_pk_row: Option<OwnedRow>,
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

    pub fn build_desc(mut descs: Vec<Field>, from_snapshot: bool) -> Vec<Field> {
        if from_snapshot {
            descs.push(Field::with_name(DataType::Varchar, "op"));
        }
        descs.push(Field::with_name(DataType::Int64, "rw_timestamp"));
        descs
    }

    pub fn create_batch_plan_for_cursor(
        table_catalog: Arc<TableCatalog>,
        session: &SessionImpl,
        context: OptimizerContextRef,
        epoch_range: Option<(u64, u64)>,
        version_id: HummockVersionId,
        seek_pk_rows: Option<OwnedRow>,
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
            for (seek_pk, (data_type, column_index)) in seek_pk_rows
                .into_inner()
                .into_vec()
                .into_iter()
                .zip_eq_fast(pks.into_iter())
            {
                if let Some(seek_pk) = seek_pk {
                    pk_rows.push(InputRef {
                        index: column_index,
                        data_type: data_type.clone(),
                    });
                    values.push((Some(seek_pk), data_type.clone()));
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

    pub fn idle_duration(&self) -> Duration {
        self.last_fetch.elapsed()
    }

    pub fn subscription_name(&self) -> &str {
        self.subscription.name.as_str()
    }

    pub fn state_info_string(&self) -> String {
        self.pg_response_stream.state_info_string()
    }
}

#[derive(Default)]
pub struct CursorManager {
    cursor_map: tokio::sync::Mutex<HashMap<String, Cursor>>,
    /// Sender clones accessible without the cursor map lock held by FETCH.
    cursor_shutdown_sender_map: Mutex<HashMap<String, ShutdownSender>>,
    shutting_down: AtomicBool,
}

impl CursorManager {
    fn register_cursor_shutdown_sender(
        &self,
        cursor_name: String,
        sender: ShutdownSender,
    ) -> Result<()> {
        let mut senders = self.cursor_shutdown_sender_map.lock();
        if self.shutting_down.load(Ordering::Acquire) {
            drop(senders);
            sender.cancel();
            return Err(SchedulerError::QueryCancelled("session ended".to_owned()).into());
        }
        senders.try_insert(cursor_name, sender).map_err(|error| {
            ErrorCode::CatalogError(format!("cursor `{}` already exists", error.entry.key()).into())
        })?;
        Ok(())
    }

    fn insert_cursor(
        &self,
        cursor_map: &mut HashMap<String, Cursor>,
        cursor_name: String,
        cursor: Cursor,
    ) -> Result<()> {
        match cursor_map.entry(cursor_name) {
            Entry::Occupied(entry) => Err(ErrorCode::CatalogError(
                format!("cursor `{}` already exists", entry.key()).into(),
            )
            .into()),
            Entry::Vacant(entry) => {
                self.register_cursor_shutdown_sender(
                    entry.key().clone(),
                    cursor.shutdown_handle().shutdown_sender(),
                )?;
                entry.insert(cursor);
                Ok(())
            }
        }
    }

    fn signal_shutdown(&self) {
        let senders = {
            // Serialize registration with terminal shutdown so no sender misses the signal.
            let mut senders = self.cursor_shutdown_sender_map.lock();
            self.shutting_down.store(true, Ordering::Release);
            mem::take(&mut *senders)
        };
        for sender in senders.into_values() {
            sender.cancel();
        }
    }

    /// Signals shutdown immediately and schedules cleanup if the cursor map is busy.
    pub(crate) fn initiate_shutdown(self: &Arc<Self>) {
        self.signal_shutdown();
        if let Ok(mut cursor_map) = self.cursor_map.try_lock() {
            cursor_map.clear();
            return;
        }
        let manager = self.clone();
        tokio::spawn(async move {
            manager.cursor_map.lock().await.clear();
        });
    }

    /// Signals shutdown and waits until every cursor in the map has been dropped.
    pub(crate) async fn shutdown_and_wait(&self) {
        self.signal_shutdown();
        self.cursor_map.lock().await.clear();
    }
}

impl CursorManager {
    /// Registers a constructed subscription cursor, rejecting insertion after shutdown.
    pub async fn add_subscription_cursor(&self, cursor: SubscriptionCursor) -> Result<()> {
        let mut cursor_map = self.cursor_map.lock().await;

        cursor_map.retain(|name, v| {
            if let Cursor::Subscription(cursor) = v
                && matches!(
                    cursor.pg_response_stream.subscription_state(),
                    SubscriptionCursorState::Invalid
                )
            {
                self.cursor_shutdown_sender_map.lock().remove(name);
                false
            } else {
                true
            }
        });

        self.insert_cursor(
            &mut cursor_map,
            cursor.cursor_name.clone(),
            Cursor::Subscription(cursor),
        )
    }

    /// Registers a constructed query cursor, rejecting insertion after shutdown.
    pub async fn add_query_cursor(&self, cursor_name: String, cursor: QueryCursor) -> Result<()> {
        let mut cursor_map = self.cursor_map.lock().await;
        self.insert_cursor(&mut cursor_map, cursor_name, Cursor::Query(cursor))
    }

    pub async fn remove_cursor(&self, cursor_name: &str) -> Result<()> {
        let mut cursor_map = self.cursor_map.lock().await;
        cursor_map.remove(cursor_name).ok_or_else(|| {
            ErrorCode::CatalogError(format!("cursor `{}` don't exists", cursor_name).into())
        })?;
        self.cursor_shutdown_sender_map.lock().remove(cursor_name);
        Ok(())
    }

    pub async fn remove_all_cursor(&self) {
        let mut cursor_map = self.cursor_map.lock().await;
        cursor_map.clear();
        self.cursor_shutdown_sender_map.lock().clear();
    }

    pub async fn remove_all_query_cursor(&self) {
        self.cursor_map.lock().await.retain(|name, cursor| {
            if matches!(cursor, Cursor::Subscription(_)) {
                true
            } else {
                self.cursor_shutdown_sender_map.lock().remove(name);
                false
            }
        });
    }

    pub async fn get_rows_with_cursor(
        &self,
        cursor_name: &str,
        count: u32,
        handler_args: HandlerArgs,
        formats: &Vec<Format>,
        timeout_seconds: Option<u64>,
        cancel_handle: &mut FetchCursorCancelHandle,
    ) -> Result<(Vec<Row>, Vec<PgFieldDescriptor>)> {
        cancel_handle.register(&handler_args.session);
        if let Some(cursor) = self.cursor_map.lock().await.get_mut(cursor_name) {
            cursor
                .fetch(count, handler_args, formats, timeout_seconds, cancel_handle)
                .await
        } else {
            Err(ErrorCode::InternalError(format!("Cannot find cursor `{}`", cursor_name)).into())
        }
    }

    pub async fn get_fields_with_cursor(&self, cursor_name: &str) -> Result<Vec<Field>> {
        if let Some(cursor) = self.cursor_map.lock().await.get_mut(cursor_name) {
            Ok(cursor.get_fields())
        } else {
            Err(ErrorCode::InternalError(format!("Cannot find cursor `{}`", cursor_name)).into())
        }
    }

    pub async fn get_periodic_cursor_metrics(&self) -> PeriodicCursorMetrics {
        let mut subscription_cursor_nums = 0;
        let mut invalid_subscription_cursor_nums = 0;
        let mut subscription_cursor_last_fetch_duration = HashMap::new();
        for cursor in self.cursor_map.lock().await.values() {
            if let Cursor::Subscription(subscription_cursor) = cursor {
                subscription_cursor_nums += 1;
                if matches!(
                    subscription_cursor.pg_response_stream.subscription_state(),
                    SubscriptionCursorState::Invalid
                ) {
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

// Statement timeouts are disabled unconditionally under madsim.
#[cfg(all(test, not(madsim)))]
mod cursor_query_startup_tests {
    use std::time::Duration;

    use futures::StreamExt;
    use risingwave_sqlparser::parser::Parser;

    use super::*;
    use crate::handler::query::{gen_batch_plan_by_statement, gen_batch_plan_fragmenter};

    /// Verifies that a local cursor query survives being left unread beyond `statement_timeout`
    /// and subsequently returns every row without a timeout error.
    #[tokio::test]
    async fn test_local_cursor_query_does_not_inherit_statement_timeout() {
        let session = Arc::new(SessionImpl::mock());
        session
            .set_config("visibility_mode", "all".to_owned())
            .unwrap();
        session
            .set_config("query_mode", "local".to_owned())
            .unwrap();
        session
            .set_config("statement_timeout", "50ms".to_owned())
            .unwrap();
        let _txn = session.txn_begin_implicit();
        // The result exceeds the output channel's capacity, keeping execution active while unread.
        let sql = "SELECT * FROM generate_series(1, 1000000)";
        let stmt = Parser::parse_sql(sql).unwrap().pop().unwrap();
        let args = HandlerArgs::new(session.clone(), &stmt, sql.into()).unwrap();
        let context = OptimizerContext::from_handler_args(args);
        let plan = gen_batch_plan_by_statement(&session, context.into(), stmt)
            .unwrap()
            .unwrap_rw()
            .unwrap();
        let fragment = gen_batch_plan_fragmenter(&session, plan).unwrap();
        assert_eq!(fragment.query_mode, QueryMode::Local);
        let snapshot = session.pinned_snapshot();
        let (mut stream, _) = create_cursor_query_stream(session, fragment, snapshot)
            .await
            .unwrap();
        let first_chunk = tokio::time::timeout(Duration::from_secs(5), stream.next())
            .await
            .expect("the local cursor query must produce its first chunk")
            .unwrap()
            .unwrap();
        let mut rows = first_chunk.cardinality();

        // Keep the cursor unread beyond the 50 ms statement timeout while backpressure keeps it
        // active. Otherwise, a fast query could finish before the deadline and hide a regression.
        tokio::time::sleep(Duration::from_millis(100)).await;

        tokio::time::timeout(Duration::from_secs(5), async {
            while let Some(chunk) = stream.next().await {
                rows += chunk
                    .expect("cursor execution must not time out")
                    .cardinality();
            }
        })
        .await
        .expect("the cursor query must finish after consumption resumes");
        assert_eq!(rows, 1000000);
    }
}

#[cfg(test)]
mod cursor_lifecycle_tests {
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use futures::StreamExt;
    use risingwave_batch::task::ShutdownToken;
    use risingwave_common::array::{DataChunk, DataChunkTestExt};
    use risingwave_common::catalog::{Field, TableId};
    use risingwave_common::error::BoxedError;
    use risingwave_sqlparser::parser::Parser;
    use tokio::sync::mpsc;
    use tokio_stream::wrappers::ReceiverStream;

    use super::{
        CursorManager, CursorQueryStream, CursorShutdownHandle, FetchCursorCancelHandle,
        FieldsManager, QueryCursor, QueryCursorDataChunkStream, QueryCursorPgResponseStream,
        SubscriptionCursor, SubscriptionCursorDataChunkStream, SubscriptionCursorHandlerContext,
        SubscriptionCursorPgResponseStream, SubscriptionCursorState,
    };
    use crate::TableCatalog;
    use crate::catalog::subscription_catalog::SubscriptionCatalog;
    use crate::handler::HandlerArgs;
    use crate::monitor::CursorMetrics;
    use crate::scheduler::QueryMessage;
    use crate::scheduler::tests::{
        create_query, running_query_execution_with_query_message_receiver,
    };
    use crate::session::SessionImpl;

    impl QueryCursor {
        /// Constructs a cursor around injected output without planning or starting an executor.
        pub(crate) fn from_query_stream_for_test(
            query_stream: CursorQueryStream,
            fields: Vec<Field>,
        ) -> Self {
            let data_stream = QueryCursorDataChunkStream::new(query_stream, fields.clone());
            Self {
                shutdown_handle: CursorShutdownHandle::new(),
                pg_response_stream: QueryCursorPgResponseStream::new(data_stream, fields),
            }
        }
    }

    impl CursorQueryStream {
        /// Creates a local cursor stream backed by `chunk_rx`, without a real executor.
        /// The shutdown pair only satisfies the wrapper's constructor: the receiver is unused,
        /// and the sender has no executor to cancel.
        pub(crate) fn local_stream_without_executor_for_test(
            chunk_rx: mpsc::Receiver<Result<DataChunk, BoxedError>>,
        ) -> Self {
            let (shutdown_tx, _shutdown_rx) = ShutdownToken::new();
            Self::local(ReceiverStream::new(chunk_rx), shutdown_tx)
        }
    }

    /// Returns the cursor shutdown token, query shutdown token, and injected chunk sender.
    /// The query token observes cancellation signalling; there is no real executor in this fixture.
    async fn add_pending_query_cursor(
        manager: &CursorManager,
        name: &str,
        fields: Vec<Field>,
    ) -> (
        ShutdownToken,
        ShutdownToken,
        mpsc::Sender<Result<DataChunk, BoxedError>>,
    ) {
        let (chunk_tx, chunk_rx) = mpsc::channel(1);
        let (shutdown_tx, shutdown_rx) = ShutdownToken::new();
        manager
            .add_query_cursor(
                name.to_owned(),
                QueryCursor::from_query_stream_for_test(
                    CursorQueryStream::local(ReceiverStream::new(chunk_rx), shutdown_tx),
                    fields,
                ),
            )
            .await
            .unwrap();
        let token = manager
            .cursor_map
            .lock()
            .await
            .get(name)
            .unwrap()
            .shutdown_handle()
            .shutdown_token();
        (token, shutdown_rx, chunk_tx)
    }

    /// Creates a subscription cursor with a test-controlled pending stream for lifecycle tests.
    /// Constructing `SubscriptionCursorState::Fetch` directly avoids the catalog lookup and snapshot query
    /// in `SubscriptionCursor::new`; `add_subscription_cursor` still exercises normal cursor and
    /// shutdown-sender registration. Returns the cursor shutdown token, query shutdown token,
    /// and injected chunk sender, like `add_pending_query_cursor`.
    async fn add_pending_subscription_cursor(
        manager: &CursorManager,
        name: &str,
        fields: FieldsManager,
    ) -> (
        ShutdownToken,
        ShutdownToken,
        mpsc::Sender<Result<DataChunk, BoxedError>>,
    ) {
        let (chunk_tx, chunk_rx) = mpsc::channel(1);
        let (shutdown_tx, shutdown_rx) = ShutdownToken::new();
        let subscription = Arc::new(SubscriptionCatalog {
            name: name.to_owned(),
            retention_seconds: 60,
            ..Default::default()
        });
        let cursor_metrics = Arc::new(CursorMetrics::for_test());
        let state = SubscriptionCursorState::Fetch {
            from_snapshot: true,
            rw_timestamp: 0,
            query_stream: CursorQueryStream::local(ReceiverStream::new(chunk_rx), shutdown_tx),
            expected_timestamp: None,
            init_query_timer: Instant::now(),
        };
        let response_state = state.strip_query_stream();
        let output_fields = fields.get_output_fields();
        let session = Arc::new(SessionImpl::mock());
        let sql = "select 1";
        let stmt = Parser::parse_sql(sql).unwrap().pop().unwrap();
        let args = HandlerArgs::new(session, &stmt, sql.into()).unwrap();
        let data_stream = SubscriptionCursorDataChunkStream::new(
            subscription.clone(),
            TableId::new(1),
            SubscriptionCursorHandlerContext::new(&args),
            fields,
            state,
            cursor_metrics.clone(),
        );
        let cursor = SubscriptionCursor {
            shutdown_handle: CursorShutdownHandle::new(),
            cursor_name: name.to_owned(),
            subscription,
            dependent_table_id: TableId::new(1),
            pg_response_stream: SubscriptionCursorPgResponseStream::new(
                data_stream,
                output_fields,
                response_state,
                Instant::now() + Duration::from_secs(60),
            ),
            cursor_metrics,
            last_fetch: Instant::now(),
        };
        let token = cursor.shutdown_handle.shutdown_token();
        manager.add_subscription_cursor(cursor).await.unwrap();
        (token, shutdown_rx, chunk_tx)
    }

    fn query_cursor_fetch_handler_args_for_test(session: Arc<SessionImpl>) -> HandlerArgs {
        let sql = "fetch 10 from cursor";
        let stmt = Parser::parse_sql(sql).unwrap().pop().unwrap();
        HandlerArgs::new(session, &stmt, sql.into()).unwrap()
    }

    /// Verifies `CancelRequest` interrupts a pending FETCH without cancelling its channel-backed
    /// cursor query, and another FETCH receives later output. No rows were consumed before cancel;
    /// this is not a real executor or a transactional replay test.
    #[tokio::test]
    async fn test_query_cursor_fetch_cancellation_preserves_cursor_query() {
        let session = Arc::new(SessionImpl::mock());
        let manager = session.get_cursor_manager();
        let (_, shutdown_rx, chunk_tx) = add_pending_query_cursor(
            &manager,
            "cursor",
            vec![Field::with_name(
                risingwave_common::types::DataType::Int32,
                "v",
            )],
        )
        .await;
        let formats = vec![];
        let mut cancel = FetchCursorCancelHandle::new();
        let mut fetch = Box::pin(manager.get_rows_with_cursor(
            "cursor",
            1,
            query_cursor_fetch_handler_args_for_test(session.clone()),
            &formats,
            None,
            &mut cancel,
        ));
        assert!(futures::poll!(fetch.as_mut()).is_pending());
        session.cancel_current_query();
        let error = tokio::time::timeout(Duration::from_secs(1), fetch)
            .await
            .unwrap()
            .unwrap_err();
        assert!(error.to_string().contains("Cancelled by user"));
        assert!(!shutdown_rx.is_cancelled());
        assert!(!chunk_tx.is_closed());
        chunk_tx
            .try_send(Ok(DataChunk::from_pretty("i\n7")))
            .unwrap();
        let (rows, _) = manager
            .get_rows_with_cursor(
                "cursor",
                1,
                query_cursor_fetch_handler_args_for_test(session),
                &formats,
                None,
                &mut FetchCursorCancelHandle::new(),
            )
            .await
            .unwrap();
        assert_eq!(rows[0].values()[0].as_deref(), Some(b"7".as_slice()));
        assert!(!shutdown_rx.is_cancelled());
        manager.remove_cursor("cursor").await.unwrap();
        assert!(shutdown_rx.is_cancelled());
    }

    /// Verifies a zero-timeout FETCH returns an immediately-ready row, and cancellation between
    /// FETCH commands makes a reused cancel handle reject the next FETCH without consuming buffered rows.
    /// No FETCH is active when cancellation is requested. This channel-backed fixture retains and
    /// reuses the registered handle rather than exercising normal per-command handle lifetimes.
    #[tokio::test]
    async fn test_query_cursor_fetch_zero_timeout_and_cancellation_between_fetches() {
        let session = Arc::new(SessionImpl::mock());
        let manager = session.get_cursor_manager();
        let (_, _shutdown_rx, chunk_tx) = add_pending_query_cursor(
            &manager,
            "cursor",
            vec![Field::with_name(
                risingwave_common::types::DataType::Int32,
                "v",
            )],
        )
        .await;
        let mut cancel = FetchCursorCancelHandle::new();
        chunk_tx
            .try_send(Ok(DataChunk::from_pretty("i\n1\n2")))
            .unwrap();
        drop(chunk_tx);
        let (rows, _) = manager
            .get_rows_with_cursor(
                "cursor",
                10,
                query_cursor_fetch_handler_args_for_test(session.clone()),
                &vec![],
                Some(0),
                &mut cancel,
            )
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].values()[0].as_deref(), Some(b"1".as_slice()));
        session.cancel_current_query();
        assert!(
            manager
                .get_rows_with_cursor(
                    "cursor",
                    1,
                    query_cursor_fetch_handler_args_for_test(session.clone()),
                    &vec![],
                    None,
                    &mut cancel,
                )
                .await
                .is_err()
        );
        let (rows, _) = manager
            .get_rows_with_cursor(
                "cursor",
                10,
                query_cursor_fetch_handler_args_for_test(session),
                &vec![],
                None,
                &mut FetchCursorCancelHandle::new(),
            )
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].values()[0].as_deref(), Some(b"2".as_slice()));
    }

    /// Verifies that a positive FETCH timeout returns accumulated rows while retaining a pending
    /// channel-backed query, and a later FETCH can continue. This does not exercise SQL or an executor.
    #[tokio::test]
    async fn test_query_cursor_fetch_timeout_returns_rows_and_preserves_cursor_query() {
        let session = Arc::new(SessionImpl::mock());
        let manager = session.get_cursor_manager();
        let (_, shutdown_rx, chunk_tx) = add_pending_query_cursor(
            &manager,
            "cursor",
            vec![Field::with_name(
                risingwave_common::types::DataType::Int32,
                "v",
            )],
        )
        .await;
        chunk_tx
            .try_send(Ok(DataChunk::from_pretty("i\n1\n2")))
            .unwrap();
        let formats = vec![];
        let mut cancel = FetchCursorCancelHandle::new();
        let started = tokio::time::Instant::now();
        let mut fetch = Box::pin(manager.get_rows_with_cursor(
            "cursor",
            3,
            query_cursor_fetch_handler_args_for_test(session.clone()),
            &formats,
            Some(1),
            &mut cancel,
        ));
        assert!(futures::poll!(fetch.as_mut()).is_pending());
        let (rows, _) = tokio::time::timeout(Duration::from_secs(5), fetch)
            .await
            .unwrap()
            .unwrap();
        assert!(started.elapsed() >= Duration::from_secs(1));
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].values()[0].as_deref(), Some(b"1".as_slice()));
        assert_eq!(rows[1].values()[0].as_deref(), Some(b"2".as_slice()));
        assert!(!shutdown_rx.is_cancelled());
        assert!(!chunk_tx.is_closed());
        chunk_tx
            .try_send(Ok(DataChunk::from_pretty("i\n3")))
            .unwrap();
        let (rows, _) = manager
            .get_rows_with_cursor(
                "cursor",
                1,
                query_cursor_fetch_handler_args_for_test(session),
                &vec![],
                None,
                &mut FetchCursorCancelHandle::new(),
            )
            .await
            .unwrap();
        assert_eq!(rows[0].values()[0].as_deref(), Some(b"3".as_slice()));
        assert!(!shutdown_rx.is_cancelled());
    }

    /// Verifies FETCH stops draining ready input after its deadline without relying on the timer
    /// driver. Blocking a current-thread runtime simulates synchronous row work, not an executor.
    // This regression needs real elapsed time without advancing the simulated timer driver.
    #[cfg(not(madsim))]
    #[tokio::test(flavor = "current_thread")]
    async fn test_fetch_rows_stops_at_deadline_with_ready_input() {
        let mut stream = futures::stream::iter((1..=3).map(|value| {
            if value == 1 {
                // Cross the deadline without yielding to the timer driver. Every input poll
                // still returns Ready, including those for the remaining rows.
                std::thread::sleep(Duration::from_millis(1100));
            }
            Ok(pgwire::types::Row::new(vec![Some(
                value.to_string().into(),
            )]))
        }));
        let rows = super::fetch_rows(
            &mut stream,
            3,
            Some(1),
            &mut FetchCursorCancelHandle::new(),
            |_| {},
        )
        .await
        .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].values()[0].as_deref(), Some(b"1".as_slice()));
        for expected in ["2", "3"] {
            let row = stream.next().await.unwrap().unwrap();
            assert_eq!(row.values()[0].as_deref(), Some(expected.as_bytes()));
        }
        assert!(stream.next().await.is_none());
    }

    /// Verifies that initiating shutdown signals all registered cursors and drops their streams
    /// before returning when the cursor map is unlocked.
    #[tokio::test]
    async fn test_cursor_shutdown_with_unlocked_map() {
        let manager = Arc::new(CursorManager::default());
        let (first, _, first_tx) = add_pending_query_cursor(&manager, "first", vec![]).await;
        let (second, _, second_tx) = add_pending_query_cursor(&manager, "second", vec![]).await;

        manager.initiate_shutdown();

        assert!(first.is_cancelled());
        assert!(second.is_cancelled());
        assert!(first_tx.is_closed());
        assert!(second_tx.is_closed());
    }

    /// Verifies that initiating shutdown signals all registered cursors without waiting for the
    /// locked cursor map, then drops their streams in the background after the lock is released.
    #[tokio::test]
    async fn test_cursor_shutdown_with_locked_map() {
        let manager = Arc::new(CursorManager::default());
        let (first, _, first_tx) = add_pending_query_cursor(&manager, "first", vec![]).await;
        let (second, _, second_tx) = add_pending_query_cursor(&manager, "second", vec![]).await;
        let cursor_map = manager.cursor_map.lock().await;

        manager.initiate_shutdown();

        assert!(first.is_cancelled());
        assert!(second.is_cancelled());
        assert!(!first_tx.is_closed());
        assert!(!second_tx.is_closed());
        drop(cursor_map);
        tokio::time::timeout(Duration::from_secs(1), async {
            first_tx.closed().await;
            second_tx.closed().await;
        })
        .await
        .expect("background cleanup must drop both cursor streams after the lock is released");
    }

    /// Verifies that awaited shutdown signals the cursor before waiting for its map lock, rejects
    /// and signals late registrations while cleanup is blocked, and returns after stream drop.
    #[tokio::test]
    async fn test_cursor_shutdown_and_wait_blocks_until_streams_are_dropped() {
        let manager = Arc::new(CursorManager::default());
        let (handle, _, chunk_tx) = add_pending_query_cursor(&manager, "cursor", vec![]).await;
        let cursor_map = manager.cursor_map.lock().await;
        let mut shutdown = Box::pin(manager.shutdown_and_wait());

        assert!(futures::poll!(shutdown.as_mut()).is_pending());
        assert!(handle.is_cancelled());
        assert!(!chunk_tx.is_closed());
        let late = CursorShutdownHandle::new();
        assert!(
            manager
                .register_cursor_shutdown_sender("late".to_owned(), late.shutdown_sender())
                .is_err()
        );
        assert!(late.shutdown_token().is_cancelled());

        drop(cursor_map);
        shutdown.await;

        assert!(chunk_tx.is_closed());
    }

    /// Verifies that shutdown interrupts pending query and subscription cursors' FETCH futures,
    /// allowing both shutdown methods to acquire the cursor-map lock and drop their result streams.
    #[tokio::test]
    async fn test_cursor_shutdown_interrupts_pending_fetch() {
        for is_subscription in [false, true] {
            for wait_for_cleanup in [false, true] {
                let session = Arc::new(SessionImpl::mock());
                let manager = session.get_cursor_manager();
                let (token, _, chunk_tx) = if is_subscription {
                    add_pending_subscription_cursor(
                        &manager,
                        "c",
                        FieldsManager::new(&TableCatalog::default()),
                    )
                    .await
                } else {
                    add_pending_query_cursor(&manager, "c", vec![]).await
                };
                let sql = "FETCH 1 FROM c";
                let stmt = Parser::parse_sql(sql).unwrap().pop().unwrap();
                let args = HandlerArgs::new(session, &stmt, sql.into()).unwrap();
                let formats = vec![];
                let mut cancel_handle = FetchCursorCancelHandle::new();
                let mut fetch = Box::pin(manager.get_rows_with_cursor(
                    "c",
                    1,
                    args,
                    &formats,
                    None,
                    &mut cancel_handle,
                ));
                assert!(futures::poll!(fetch.as_mut()).is_pending());
                assert!(manager.cursor_map.try_lock().is_err());

                let result = if wait_for_cleanup {
                    let mut shutdown = Box::pin(manager.shutdown_and_wait());
                    assert!(futures::poll!(shutdown.as_mut()).is_pending());
                    tokio::time::timeout(Duration::from_secs(1), async {
                        let (result, ()) = tokio::join!(fetch, shutdown);
                        result
                    })
                    .await
                    .expect("FETCH must release the lock so awaited shutdown can finish")
                } else {
                    manager.initiate_shutdown();
                    tokio::time::timeout(Duration::from_secs(1), fetch)
                        .await
                        .expect("initiated shutdown must interrupt FETCH")
                };

                assert!(result.unwrap_err().to_string().contains("cursor closed"));
                assert!(token.is_cancelled());
                tokio::time::timeout(Duration::from_secs(1), chunk_tx.closed())
                    .await
                    .expect("shutdown must drop the cursor stream after FETCH releases the lock");
            }
        }
    }

    /// Verifies that a cursor creation already waiting for the map lock is rejected after terminal
    /// shutdown begins, and its unregistered result stream is dropped.
    #[tokio::test]
    async fn test_cursor_shutdown_rejects_creation_waiting_for_map() {
        let manager = Arc::new(CursorManager::default());
        let cursor_map = manager.cursor_map.lock().await;
        let (chunk_tx, chunk_rx) = mpsc::channel(1);
        let mut adding = Box::pin(manager.add_query_cursor(
            "late".to_owned(),
            QueryCursor::from_query_stream_for_test(
                CursorQueryStream::local_stream_without_executor_for_test(chunk_rx),
                vec![],
            ),
        ));
        assert!(futures::poll!(adding.as_mut()).is_pending());

        manager.initiate_shutdown();
        drop(cursor_map);

        let error = tokio::time::timeout(Duration::from_secs(1), adding)
            .await
            .expect("cursor creation must finish after the lock is released")
            .unwrap_err();
        assert!(error.to_string().contains("session ended"));
        assert!(chunk_tx.is_closed());
        manager.shutdown_and_wait().await;
        assert!(manager.cursor_map.lock().await.is_empty());
        assert!(manager.cursor_shutdown_sender_map.lock().is_empty());
    }

    /// Verifies that declaring a duplicate name fails and drops the new cursor's stream without
    /// signaling the existing cursor or dropping its stream.
    #[tokio::test]
    async fn test_duplicate_cursor_name_rejects_new_cursor_and_preserves_existing_cursor() {
        let manager = Arc::new(CursorManager::default());
        let (query, _, query_tx) = add_pending_query_cursor(&manager, "query", vec![]).await;
        let (duplicate_tx, duplicate_rx) = mpsc::channel(1);

        assert!(
            manager
                .add_query_cursor(
                    "query".to_owned(),
                    QueryCursor::from_query_stream_for_test(
                        CursorQueryStream::local_stream_without_executor_for_test(duplicate_rx),
                        vec![],
                    ),
                )
                .await
                .is_err()
        );

        assert!(duplicate_tx.is_closed());
        assert!(!query.is_cancelled());
        assert!(!query_tx.is_closed());
    }

    /// Verifies that shutting down one cursor via CLOSE signals it and drops its stream, leaves
    /// another cursor untouched, and permits a new live cursor to reuse the closed cursor's name.
    #[tokio::test]
    async fn test_cursor_shutdown_preserves_other_cursor_and_allows_name_reuse() {
        let manager = Arc::new(CursorManager::default());
        let (query, _, query_tx) = add_pending_query_cursor(&manager, "query", vec![]).await;
        let (subscription, _, subscription_tx) = add_pending_subscription_cursor(
            &manager,
            "subscription",
            FieldsManager::new(&TableCatalog::default()),
        )
        .await;

        manager.remove_cursor("query").await.unwrap();

        assert!(query.is_cancelled());
        assert!(query_tx.is_closed());
        assert!(!subscription.is_cancelled());
        assert!(!subscription_tx.is_closed());
        let (replacement, _, replacement_tx) =
            add_pending_query_cursor(&manager, "query", vec![]).await;
        assert!(!replacement.is_cancelled());
        assert!(!replacement_tx.is_closed());
    }

    /// Verifies that query-only cleanup signals query cursors and drops their streams while
    /// leaving subscription cursors unsignalled with their streams still open.
    #[tokio::test]
    async fn test_all_query_cursors_shutdown_preserves_subscription_cursors() {
        let manager = Arc::new(CursorManager::default());
        let (query, _, query_tx) = add_pending_query_cursor(&manager, "query", vec![]).await;
        let (subscription, _, subscription_tx) = add_pending_subscription_cursor(
            &manager,
            "subscription",
            FieldsManager::new(&TableCatalog::default()),
        )
        .await;

        manager.remove_all_query_cursor().await;

        assert!(query.is_cancelled());
        assert!(query_tx.is_closed());
        assert!(!subscription.is_cancelled());
        assert!(!subscription_tx.is_closed());
    }

    /// Verifies that CLOSE ALL signals both cursor types and drops their streams without ending
    /// the manager's lifetime: new live cursors can be declared using both removed names.
    #[tokio::test]
    async fn test_all_cursors_shutdown_allows_new_declarations() {
        let manager = Arc::new(CursorManager::default());
        let (query, _, query_tx) = add_pending_query_cursor(&manager, "query", vec![]).await;
        let (subscription, _, subscription_tx) = add_pending_subscription_cursor(
            &manager,
            "subscription",
            FieldsManager::new(&TableCatalog::default()),
        )
        .await;

        manager.remove_all_cursor().await;

        assert!(query.is_cancelled());
        assert!(query_tx.is_closed());
        assert!(subscription.is_cancelled());
        assert!(subscription_tx.is_closed());
        let (new_query, _, new_query_tx) =
            add_pending_query_cursor(&manager, "query", vec![]).await;
        let (new_subscription, _, new_subscription_tx) = add_pending_subscription_cursor(
            &manager,
            "subscription",
            FieldsManager::new(&TableCatalog::default()),
        )
        .await;
        assert!(!new_query.is_cancelled());
        assert!(!new_query_tx.is_closed());
        assert!(!new_subscription.is_cancelled());
        assert!(!new_subscription_tx.is_closed());
    }

    /// Verifies that unfinished local cursor shutdown via CLOSE signals its executor token and drops
    /// its output stream without signaling the ordinary local query. Shutdown must work both
    /// before and after the first FETCH initializes row formatting in the persistent adapter.
    #[tokio::test]
    async fn test_local_cursor_shutdown_preserves_ordinary_query() {
        // Simulate shutdown before the first FETCH (false) or after it initializes the row stream
        // (true), without executing FETCH itself.
        for initialize_rows in [false, true] {
            let session = Arc::new(SessionImpl::mock());
            let ordinary_shutdown = session.reset_cancel_query_flag();
            let (shutdown_tx, shutdown_rx) = ShutdownToken::new();
            let (chunk_tx, chunk_rx) = mpsc::channel(1);
            let mut stream = CursorQueryStream::local(ReceiverStream::new(chunk_rx), shutdown_tx);
            assert!(futures::poll!(stream.next()).is_pending());
            let mut cursor = QueryCursor::from_query_stream_for_test(stream, vec![]);
            if initialize_rows {
                cursor.pg_response_stream.begin_fetch(&[], &session);
            }
            let manager = session.get_cursor_manager();
            manager
                .add_query_cursor("cursor".to_owned(), cursor)
                .await
                .unwrap();

            assert!(!shutdown_rx.is_cancelled());
            manager.remove_cursor("cursor").await.unwrap();

            assert!(shutdown_rx.is_cancelled());
            assert!(chunk_tx.is_closed());
            assert!(!ordinary_shutdown.is_cancelled());
            session.cancel_current_query();
            assert!(ordinary_shutdown.is_cancelled());
        }
    }

    /// Verifies that a local cursor query stream forwards chunks and does not request
    /// cancellation when dropped after EOF.
    #[tokio::test]
    async fn test_completed_local_cursor_query_does_not_signals_shutdown() {
        let (shutdown_tx, shutdown_rx) = ShutdownToken::new();
        let (chunk_tx, chunk_rx) = mpsc::channel(1);
        let chunk = DataChunk::from_pretty("i\n1\n2");
        chunk_tx.try_send(Ok(chunk.clone())).unwrap();
        drop(chunk_tx);
        let mut stream = CursorQueryStream::local(ReceiverStream::new(chunk_rx), shutdown_tx);

        assert_eq!(stream.next().await.unwrap().unwrap(), chunk);
        assert!(stream.next().await.is_none());
        drop(stream);

        assert!(!shutdown_rx.is_cancelled());
    }

    /// Verifies that unfinished distributed cursor shutdown via CLOSE requests cancellation and
    /// removes its global and session registrations. Another registered query remains owned and
    /// receives no cancellation request. Shutdown must work both before and after the first FETCH
    /// initializes row formatting in the persistent adapter.
    #[tokio::test]
    async fn test_distributed_cursor_shutdown_cancels_only_owned_query() {
        // Simulate shutdown before the first FETCH (false) or after it initializes the row stream
        // (true), without executing FETCH itself.
        for initialize_rows in [false, true] {
            let session = Arc::new(SessionImpl::mock());
            let manager = session.env().query_manager().clone();
            let query = create_query().await;
            let query_id = query.query_id().clone();
            let (execution, mut control_rx) =
                running_query_execution_with_query_message_receiver(query);
            manager.add_query(query_id.clone(), execution);
            session.register_distributed_query(query_id.clone(), true);
            let other_query = create_query().await;
            let other_id = other_query.query_id().clone();
            let (other_execution, mut other_control_rx) =
                running_query_execution_with_query_message_receiver(other_query);
            manager.add_query(other_id.clone(), other_execution);
            session.register_distributed_query(other_id.clone(), false);
            let (_chunk_tx, chunk_rx) = mpsc::channel(1);
            let mut stream = CursorQueryStream::distributed(
                manager.query_stream_for_test(query_id.clone(), chunk_rx, Arc::downgrade(&session)),
                manager.clone(),
            );
            assert!(futures::poll!(stream.next()).is_pending());
            let mut cursor = QueryCursor::from_query_stream_for_test(stream, vec![]);
            if initialize_rows {
                cursor.pg_response_stream.begin_fetch(&[], &session);
                assert!(futures::poll!(cursor.pg_response_stream.next()).is_pending());
            }
            let cursors = session.get_cursor_manager();
            cursors
                .add_query_cursor("cursor".to_owned(), cursor)
                .await
                .unwrap();

            cursors.remove_cursor("cursor").await.unwrap();

            let message = tokio::time::timeout(Duration::from_secs(1), control_rx.recv())
                .await
                .expect("closing the cursor must request query cancellation")
                .expect("query cancellation message must arrive");
            assert!(
                matches!(message, QueryMessage::CancelQuery(reason) if reason == "cursor closed")
            );
            assert!(!manager.contains_query_for_test(&query_id));
            assert_eq!(session.all_distributed_query_ids(), vec![other_id.clone()]);
            assert_eq!(
                session.ordinary_distributed_query_ids(),
                vec![other_id.clone()]
            );
            assert!(manager.contains_query_for_test(&other_id));
            assert!(
                tokio::time::timeout(Duration::from_secs(1), other_control_rx.recv())
                    .await
                    .is_err(),
                "closing one cursor must not cancel another registered query"
            );
        }
    }

    /// Verifies that a distributed cursor query stream forwards chunks and releases its global
    /// and session registrations without cancellation when dropped after EOF.
    #[tokio::test]
    async fn test_completed_distributed_cursor_query_does_not_request_query_cancellation() {
        let session = Arc::new(SessionImpl::mock());
        let manager = session.env().query_manager().clone();
        let query = create_query().await;
        let query_id = query.query_id().clone();
        let (execution, mut control_rx) =
            running_query_execution_with_query_message_receiver(query);
        manager.add_query(query_id.clone(), execution.clone());
        session.register_distributed_query(query_id.clone(), true);
        let (chunk_tx, chunk_rx) = mpsc::channel(1);
        let chunk = DataChunk::from_pretty("i\n1\n2");
        chunk_tx.try_send(Ok(chunk.clone())).unwrap();
        drop(chunk_tx);
        let mut stream = CursorQueryStream::distributed(
            manager.query_stream_for_test(query_id.clone(), chunk_rx, Arc::downgrade(&session)),
            manager.clone(),
        );

        assert_eq!(stream.next().await.unwrap().unwrap(), chunk);
        assert!(stream.next().await.is_none());
        drop(stream);

        assert!(!manager.contains_query_for_test(&query_id));
        assert!(session.all_distributed_query_ids().is_empty());
        assert!(
            tokio::time::timeout(Duration::from_secs(1), control_rx.recv())
                .await
                .is_err(),
            "dropping a completed stream must not request query cancellation"
        );
        drop(execution);
    }

    /// Verifies that each sender targets only its own cursor and that retained sender clones
    /// can shut down all cursors independently of their handles.
    #[test]
    fn test_cursor_shutdown_handles_are_independent() {
        let first = CursorShutdownHandle::new();
        let second = CursorShutdownHandle::new();
        let first_rx = first.shutdown_token();
        let second_rx = second.shutdown_token();
        let senders = [first.shutdown_sender(), second.shutdown_sender()];

        senders[0].cancel();
        assert!(first_rx.is_cancelled());
        assert!(first.shutdown_token().is_cancelled());
        assert!(!second_rx.is_cancelled());

        // A manager can shut down all cursors through retained sender clones.
        for sender in senders {
            sender.cancel();
        }
        assert!(first_rx.is_cancelled());
        assert!(second_rx.is_cancelled());
    }

    /// Verifies that the receiver observes cancellation when the shutdown handle is dropped.
    #[test]
    fn test_cursor_shutdown_handle_drop_signals_shutdown() {
        let handle = CursorShutdownHandle::new();
        let shutdown_rx = handle.shutdown_token();
        assert!(!shutdown_rx.is_cancelled());

        drop(handle);

        assert!(shutdown_rx.is_cancelled());
    }
}
