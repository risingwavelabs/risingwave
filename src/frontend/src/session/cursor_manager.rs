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
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt::{Display, Formatter};
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
use pgwire::types::{Format, Row};
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
use crate::handler::declare_cursor::create_chunk_stream_for_cursor;
use crate::handler::query::{RwBatchQueryPlanResult, gen_batch_plan_fragmenter};
use crate::handler::util::{
    DataChunkToRowSetAdapter, StaticSessionData, convert_logstore_u64_to_unix_millis,
    pg_value_format, to_pg_field,
};
use crate::monitor::{CursorMetrics, PeriodicCursorMetrics};
use crate::optimizer::PlanRoot;
use crate::optimizer::plan_node::{BatchFilter, BatchLogSeqScan, BatchSeqScan, generic};
use crate::optimizer::property::{Order, RequiredDist};
use crate::scheduler::{
    DistributedQueryStream, LocalQueryStream, QueryManager, ReadSnapshot, SchedulerError,
};
use crate::utils::Condition;
use crate::{OptimizerContext, OptimizerContextRef, PgResponseStream, TableCatalog};

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

pub enum CursorDataChunkStream {
    LocalDataChunk(Option<CursorQueryStream>),
    DistributedDataChunk(Option<CursorQueryStream>),
    PgResponse(PgResponseStream),
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

    fn is_cancelled(&self) -> bool {
        self.cancel_rx.is_cancelled()
    }
}

impl CursorDataChunkStream {
    pub fn init_row_stream(
        &mut self,
        fields: &Vec<Field>,
        formats: &Vec<Format>,
        session: Arc<SessionImpl>,
    ) {
        let columns_type = fields.iter().map(|f| f.data_type()).collect();
        match self {
            CursorDataChunkStream::LocalDataChunk(data_chunk)
            | CursorDataChunkStream::DistributedDataChunk(data_chunk) => {
                let data_chunk = mem::take(data_chunk).unwrap();
                let row_stream = PgResponseStream::Rows(
                    DataChunkToRowSetAdapter::new(
                        data_chunk,
                        columns_type,
                        formats.clone(),
                        session,
                    )
                    .boxed(),
                );
                *self = CursorDataChunkStream::PgResponse(row_stream);
            }
            _ => {}
        }
    }

    pub async fn next(&mut self) -> Result<Option<std::result::Result<Vec<Row>, BoxedError>>> {
        match self {
            CursorDataChunkStream::PgResponse(row_stream) => Ok(row_stream.next().await),
            _ => Err(ErrorCode::InternalError(
                "Only 'CursorDataChunkStream' can call next and return rows".to_owned(),
            )
            .into()),
        }
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

    pub async fn next(
        &mut self,
        count: u32,
        handler_args: HandlerArgs,
        formats: &Vec<Format>,
        timeout_seconds: Option<u64>,
        cancel_handle: &mut FetchCursorCancelHandle,
    ) -> Result<(Vec<Row>, Vec<PgFieldDescriptor>)> {
        match self {
            Cursor::Subscription(cursor) => cursor
                .next(count, handler_args, formats, timeout_seconds, cancel_handle)
                .await
                .inspect_err(|_| cursor.cursor_metrics.subscription_cursor_error_count.inc()),
            Cursor::Query(cursor) => {
                cursor
                    .next(count, formats, handler_args, timeout_seconds)
                    .await
            }
        }
    }

    pub fn get_fields(&mut self) -> Vec<Field> {
        match self {
            Cursor::Subscription(cursor) => cursor.fields_manager.get_output_fields(),
            Cursor::Query(cursor) => cursor.fields.clone(),
        }
    }
}

pub struct QueryCursor {
    shutdown_handle: CursorShutdownHandle,
    chunk_stream: CursorDataChunkStream,
    fields: Vec<Field>,
    remaining_rows: VecDeque<Row>,
}

impl QueryCursor {
    pub fn new(chunk_stream: CursorDataChunkStream, fields: Vec<Field>) -> Result<Self> {
        Ok(Self {
            shutdown_handle: CursorShutdownHandle::new(),
            chunk_stream,
            fields,
            remaining_rows: VecDeque::<Row>::new(),
        })
    }

    pub async fn next_once(&mut self) -> Result<Option<Row>> {
        while self.remaining_rows.is_empty() {
            let rows = self.chunk_stream.next().await?;
            let rows = match rows {
                None => return Ok(None),
                Some(row) => row?,
            };
            self.remaining_rows = rows.into_iter().collect();
        }
        let row = self.remaining_rows.pop_front().unwrap();
        Ok(Some(row))
    }

    pub async fn next(
        &mut self,
        count: u32,
        formats: &Vec<Format>,
        handler_args: HandlerArgs,
        timeout_seconds: Option<u64>,
    ) -> Result<(Vec<Row>, Vec<PgFieldDescriptor>)> {
        // `FETCH NEXT` is equivalent to `FETCH 1`.
        // min with 100 to avoid allocating too many memory at once.
        let timeout_instant = timeout_seconds.map(|s| Instant::now() + Duration::from_secs(s));
        let session = handler_args.session;
        let mut ans = Vec::with_capacity(std::cmp::min(100, count) as usize);
        let mut cur = 0;
        let desc = self.fields.iter().map(to_pg_field).collect();
        self.chunk_stream
            .init_row_stream(&self.fields, formats, session);
        while cur < count
            && let Some(row) = self.next_once().await?
        {
            cur += 1;
            ans.push(row);
            if let Some(timeout_instant) = timeout_instant
                && Instant::now() > timeout_instant
            {
                break;
            }
        }
        Ok((ans, desc))
    }
}

enum State {
    InitLogStoreQuery {
        // The rw_timestamp used to initiate the query to read from subscription logstore.
        seek_timestamp: u64,

        // If specified, the expected_timestamp must be an exact match for the next rw_timestamp.
        expected_timestamp: Option<u64>,
    },
    Fetch {
        // Whether the query is reading from snapshot
        // true: read from the upstream table snapshot
        // false: read from subscription logstore
        from_snapshot: bool,

        // The rw_timestamp used to initiate the query to read from subscription logstore.
        rw_timestamp: u64,

        // The row stream to from the batch query read.
        // It is returned from the batch execution.
        chunk_stream: CursorDataChunkStream,

        // A cache to store the remaining rows from the row stream.
        remaining_rows: VecDeque<Row>,

        expected_timestamp: Option<u64>,

        init_query_timer: Instant,
    },
    Invalid,
}

impl Display for State {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            State::InitLogStoreQuery {
                seek_timestamp,
                expected_timestamp,
            } => write!(
                f,
                "InitLogStoreQuery {{ seek_timestamp: {}, expected_timestamp: {:?} }}",
                seek_timestamp, expected_timestamp
            ),
            State::Fetch {
                from_snapshot,
                rw_timestamp,
                expected_timestamp,
                remaining_rows,
                init_query_timer,
                ..
            } => write!(
                f,
                "Fetch {{ from_snapshot: {}, rw_timestamp: {}, expected_timestamp: {:?}, cached rows: {}, query init at {}ms before }}",
                from_snapshot,
                rw_timestamp,
                expected_timestamp,
                remaining_rows.len(),
                init_query_timer.elapsed().as_millis()
            ),
            State::Invalid => write!(f, "Invalid"),
        }
    }
}

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
        let mut row_pk_indices = Vec::new();
        let mut stream_chunk_row_indices = Vec::new();
        let mut output_idx = 0_usize;
        let pk_set: HashSet<usize> = catalog
            .pk
            .iter()
            .map(|col_order| col_order.column_index)
            .collect();

        for (index, v) in catalog.columns.iter().enumerate() {
            if pk_set.contains(&index) {
                row_pk_indices.push(output_idx);
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

    pub fn process_output_desc_row(&self, mut rows: Vec<Row>) -> (Vec<Row>, Option<Row>) {
        let last_row = rows.last_mut().map(|row| {
            let mut row = row.clone();
            row.project(&self.row_pk_indices)
        });
        let rows = rows
            .iter_mut()
            .map(|row| row.project(&self.row_output_col_indices))
            .collect();
        (rows, last_row)
    }

    pub fn get_output_fields(&self) -> Vec<Field> {
        self.row_output_col_indices
            .iter()
            .map(|&idx| self.row_fields[idx].clone())
            .collect()
    }

    // In the beginning (declare cur), we will give it an empty formats,
    // this formats is not a real, when we fetch, We fill it with the formats returned from the pg client.
    pub fn get_row_stream_fields_and_formats(
        &self,
        formats: &Vec<Format>,
        from_snapshot: bool,
    ) -> (Vec<Field>, Vec<Format>) {
        let mut fields = Vec::new();
        let need_format = !(formats.is_empty() || formats.len() == 1);
        let mut new_formats = formats.clone();
        let stream_chunk_row_indices_iter = if from_snapshot {
            self.stream_chunk_row_indices.iter().chain(None)
        } else {
            self.stream_chunk_row_indices
                .iter()
                .chain(Some(&self.op_index))
        };
        for index in stream_chunk_row_indices_iter {
            fields.push(self.row_fields[*index].clone());
            if need_format && !self.row_output_col_indices.contains(index) {
                new_formats.insert(*index, Format::Text);
            }
        }
        (fields, new_formats)
    }
}

pub struct SubscriptionCursor {
    shutdown_handle: CursorShutdownHandle,
    cursor_name: String,
    subscription: Arc<SubscriptionCatalog>,
    dependent_table_id: TableId,
    cursor_need_drop_time: Instant,
    state: State,
    // fields will be set in the table's catalog when the cursor is created,
    // and will be reset each time it is created chunk_stream, this is to avoid changes in the catalog due to alter.
    fields_manager: FieldsManager,
    cursor_metrics: Arc<CursorMetrics>,
    last_fetch: Instant,
    seek_pk_row: Option<Row>,
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
        let (state, fields_manager) = if let Some(start_timestamp) = start_timestamp {
            let table_catalog = handler_args.session.get_table_by_id(dependent_table_id)?;
            (
                State::InitLogStoreQuery {
                    seek_timestamp: start_timestamp,
                    expected_timestamp: None,
                },
                FieldsManager::new(&table_catalog),
            )
        } else {
            // The query stream needs to initiated on cursor creation to make sure
            // future fetch on the cursor starts from the snapshot when the cursor is declared.
            //
            // TODO: is this the right behavior? Should we delay the query stream initiation till the first fetch?
            let (chunk_stream, init_query_timer, table_catalog) =
                Self::initiate_query(None, dependent_table_id, handler_args.clone(), None).await?;
            let pinned_epoch = match handler_args.session.get_pinned_snapshot().ok_or_else(
                || ErrorCode::InternalError("Fetch Cursor can't find snapshot epoch".to_owned()),
            )? {
                ReadSnapshot::FrontendPinned { snapshot, .. } => {
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
            let start_timestamp = pinned_epoch;

            (
                State::Fetch {
                    from_snapshot: true,
                    rw_timestamp: start_timestamp,
                    chunk_stream,
                    remaining_rows: VecDeque::new(),
                    expected_timestamp: None,
                    init_query_timer,
                },
                FieldsManager::new(&table_catalog),
            )
        };

        let cursor_need_drop_time =
            Instant::now() + Duration::from_secs(subscription.retention_seconds);
        Ok(Self {
            shutdown_handle: CursorShutdownHandle::new(),
            cursor_name,
            subscription,
            dependent_table_id,
            cursor_need_drop_time,
            state,
            fields_manager,
            cursor_metrics,
            last_fetch: Instant::now(),
            seek_pk_row: None,
        })
    }

    async fn next_row(
        &mut self,
        handler_args: &HandlerArgs,
        formats: &Vec<Format>,
    ) -> Result<Option<Row>> {
        loop {
            match &mut self.state {
                State::InitLogStoreQuery {
                    seek_timestamp,
                    expected_timestamp,
                } => {
                    let from_snapshot = false;

                    // Initiate a new batch query to continue fetching
                    match Self::get_next_rw_timestamp(
                        *seek_timestamp,
                        self.dependent_table_id,
                        *expected_timestamp,
                        handler_args.clone(),
                        &self.subscription,
                    )
                    .await
                    {
                        Ok((Some(rw_timestamp), expected_timestamp)) => {
                            let (mut chunk_stream, init_query_timer, catalog) =
                                Self::initiate_query(
                                    Some(rw_timestamp),
                                    self.dependent_table_id,
                                    handler_args.clone(),
                                    None,
                                )
                                .await?;
                            let table_schema_changed =
                                self.fields_manager.try_refill_fields(&catalog);
                            let (fields, formats) = self
                                .fields_manager
                                .get_row_stream_fields_and_formats(formats, from_snapshot);
                            chunk_stream.init_row_stream(
                                &fields,
                                &formats,
                                handler_args.session.clone(),
                            );

                            self.cursor_need_drop_time = Instant::now()
                                + Duration::from_secs(self.subscription.retention_seconds);
                            let mut remaining_rows = VecDeque::new();
                            Self::try_refill_remaining_rows(&mut chunk_stream, &mut remaining_rows)
                                .await?;
                            // Transition to the Fetch state
                            self.state = State::Fetch {
                                from_snapshot,
                                rw_timestamp,
                                chunk_stream,
                                remaining_rows,
                                expected_timestamp,
                                init_query_timer,
                            };
                            if table_schema_changed {
                                return Ok(None);
                            }
                        }
                        Ok((None, _)) => return Ok(None),
                        Err(e) => {
                            self.state = State::Invalid;
                            return Err(e);
                        }
                    }
                }
                State::Fetch {
                    from_snapshot,
                    rw_timestamp,
                    chunk_stream,
                    remaining_rows,
                    expected_timestamp,
                    init_query_timer,
                } => {
                    let session_data = StaticSessionData {
                        timezone: handler_args.session.config().timezone(),
                    };
                    let from_snapshot = *from_snapshot;
                    let rw_timestamp = *rw_timestamp;

                    // Try refill remaining rows
                    Self::try_refill_remaining_rows(chunk_stream, remaining_rows).await?;

                    if let Some(row) = remaining_rows.pop_front() {
                        // 1. Fetch the next row
                        if from_snapshot {
                            return Ok(Some(Self::build_row(
                                row.take(),
                                None,
                                formats,
                                &session_data,
                            )?));
                        } else {
                            return Ok(Some(Self::build_row(
                                row.take(),
                                Some(rw_timestamp),
                                formats,
                                &session_data,
                            )?));
                        }
                    } else {
                        self.cursor_metrics
                            .subscription_cursor_query_duration
                            .with_label_values(&[&self.subscription.name])
                            .observe(init_query_timer.elapsed().as_millis() as _);
                        // 2. Reach EOF for the current query.
                        if let Some(expected_timestamp) = expected_timestamp {
                            self.state = State::InitLogStoreQuery {
                                seek_timestamp: *expected_timestamp,
                                expected_timestamp: Some(*expected_timestamp),
                            };
                        } else {
                            self.state = State::InitLogStoreQuery {
                                seek_timestamp: rw_timestamp + 1,
                                expected_timestamp: None,
                            };
                        }
                    }
                }
                State::Invalid => {
                    // TODO: auto close invalid cursor?
                    return Err(ErrorCode::InternalError(
                        "Cursor is in invalid state. Please close and re-create the cursor."
                            .to_owned(),
                    )
                    .into());
                }
            }
        }
    }

    pub async fn next(
        &mut self,
        count: u32,
        handler_args: HandlerArgs,
        formats: &Vec<Format>,
        timeout_seconds: Option<u64>,
        cancel_handle: &mut FetchCursorCancelHandle,
    ) -> Result<(Vec<Row>, Vec<PgFieldDescriptor>)> {
        let timeout_instant = timeout_seconds.map(|s| Instant::now() + Duration::from_secs(s));
        if Instant::now() > self.cursor_need_drop_time {
            return Err(ErrorCode::InternalError(
                "The cursor has exceeded its maximum lifetime, please recreate it (close then declare cursor).".to_owned(),
            )
            .into());
        }

        let session = &handler_args.session;
        let mut ans = Vec::with_capacity(std::cmp::min(100, count) as usize);
        let mut cur = 0;
        if let State::Fetch {
            from_snapshot,
            chunk_stream,
            ..
        } = &mut self.state
        {
            let (fields, fotmats) = self
                .fields_manager
                .get_row_stream_fields_and_formats(formats, *from_snapshot);
            chunk_stream.init_row_stream(&fields, &fotmats, session.clone());
        }
        while cur < count {
            if cancel_handle.is_cancelled() {
                return Err(SchedulerError::QueryCancelled("Cancelled by user".to_owned()).into());
            }
            let fetch_cursor_timer = Instant::now();
            let row = self.next_row(&handler_args, formats).await?;
            self.cursor_metrics
                .subscription_cursor_fetch_duration
                .with_label_values(&[&self.subscription.name])
                .observe(fetch_cursor_timer.elapsed().as_millis() as _);
            match row {
                Some(row) => {
                    cur += 1;
                    ans.push(row);
                }
                None => {
                    let timeout_seconds = timeout_seconds.unwrap_or(0);
                    if cur > 0 || timeout_seconds == 0 {
                        break;
                    }
                    let State::InitLogStoreQuery { seek_timestamp, .. } = &self.state else {
                        // Triggered when previous next_row returns None while self.state is State::Fetch.
                        continue;
                    };
                    // This is the only point where subscription cursor fetch waits without an
                    // inner query. Register the FETCH-level cancel token so CancelRequest can
                    // interrupt this wait. The token also marks the whole FETCH as cancelled, so
                    // we won't start another inner query after a cancellation.
                    cancel_handle.register(session);
                    let timeout = tokio::time::sleep(Duration::from_secs(timeout_seconds));
                    tokio::pin!(timeout);
                    tokio::select! {
                        biased;
                        _ = cancel_handle.cancelled() => {
                            return Err(SchedulerError::QueryCancelled(
                                "Cancelled by user".to_owned(),
                            )
                            .into());
                        }
                        result = session
                            .env
                            .hummock_snapshot_manager()
                            .wait_table_change_log_notification(
                                self.dependent_table_id,
                                *seek_timestamp,
                            ) => {
                            result?;
                        }
                        _ = &mut timeout => {
                            tracing::debug!("Cursor wait next epoch timeout");
                            break;
                        }
                    }
                    if cancel_handle.is_cancelled() {
                        return Err(
                            SchedulerError::QueryCancelled("Cancelled by user".to_owned()).into(),
                        );
                    }
                }
            }
            // Timeout, return with current value
            if let Some(timeout_instant) = timeout_instant
                && Instant::now() > timeout_instant
            {
                break;
            }
        }
        self.last_fetch = Instant::now();
        let (rows, seek_pk_row) = self.fields_manager.process_output_desc_row(ans);
        if let Some(seek_pk_row) = seek_pk_row {
            self.seek_pk_row = Some(seek_pk_row);
        }
        let desc = self
            .fields_manager
            .get_output_fields()
            .iter()
            .map(to_pg_field)
            .collect();

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
        match self.state {
            // Only used to return generated plans, so rw_timestamp are meaningless
            State::InitLogStoreQuery { .. } => Self::init_batch_plan_for_subscription_cursor(
                Some(0),
                self.dependent_table_id,
                handler_args,
                self.seek_pk_row.clone(),
            ),
            State::Fetch {
                from_snapshot,
                rw_timestamp,
                ..
            } => {
                if from_snapshot {
                    Self::init_batch_plan_for_subscription_cursor(
                        None,
                        self.dependent_table_id,
                        handler_args,
                        self.seek_pk_row.clone(),
                    )
                } else {
                    Self::init_batch_plan_for_subscription_cursor(
                        Some(rw_timestamp),
                        self.dependent_table_id,
                        handler_args,
                        self.seek_pk_row.clone(),
                    )
                }
            }
            State::Invalid => Err(ErrorCode::InternalError(
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
        seek_pk_row: Option<Row>,
    ) -> Result<(CursorDataChunkStream, Instant, Arc<TableCatalog>)> {
        let init_query_timer = Instant::now();
        let session = handler_args.clone().session;
        let table_catalog = session.get_table_by_id(dependent_table_id)?;
        let plan_result = Self::init_batch_plan_for_subscription_cursor(
            rw_timestamp,
            dependent_table_id,
            handler_args.clone(),
            seek_pk_row,
        )?;
        let plan_fragmenter_result = gen_batch_plan_fragmenter(&handler_args.session, plan_result)?;
        let (chunk_stream, _) =
            create_chunk_stream_for_cursor(handler_args.session, plan_fragmenter_result).await?;
        Ok((chunk_stream, init_query_timer, table_catalog))
    }

    async fn try_refill_remaining_rows(
        chunk_stream: &mut CursorDataChunkStream,
        remaining_rows: &mut VecDeque<Row>,
    ) -> Result<()> {
        if remaining_rows.is_empty()
            && let Some(row_set) = chunk_stream.next().await?
        {
            remaining_rows.extend(row_set?);
        }
        Ok(())
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

    pub fn idle_duration(&self) -> Duration {
        self.last_fetch.elapsed()
    }

    pub fn subscription_name(&self) -> &str {
        self.subscription.name.as_str()
    }

    pub fn state_info_string(&self) -> String {
        format!("{}", self.state)
    }
}

pub struct CursorManager {
    cursor_map: tokio::sync::Mutex<HashMap<String, Cursor>>,
    /// Sender clones accessible without the cursor map lock held by FETCH.
    cursor_shutdown_sender_map: Mutex<HashMap<String, ShutdownSender>>,
    shutting_down: AtomicBool,
    cursor_metrics: Arc<CursorMetrics>,
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
    pub fn new(cursor_metrics: Arc<CursorMetrics>) -> Self {
        Self {
            cursor_map: tokio::sync::Mutex::new(HashMap::new()),
            cursor_shutdown_sender_map: Mutex::new(HashMap::new()),
            shutting_down: AtomicBool::new(false),
            cursor_metrics,
        }
    }

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

        cursor_map.retain(|name, v| {
            if let Cursor::Subscription(cursor) = v
                && matches!(cursor.state, State::Invalid)
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

    pub async fn add_query_cursor(
        &self,
        cursor_name: String,
        chunk_stream: CursorDataChunkStream,
        fields: Vec<Field>,
    ) -> Result<()> {
        let cursor = QueryCursor::new(chunk_stream, fields)?;
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
        if let Some(cursor) = self.cursor_map.lock().await.get_mut(cursor_name) {
            let mut shutdown_rx = cursor.shutdown_handle().shutdown_token();
            // Dropping FETCH's future is safe for terminal shutdown because the cursor is discarded.
            tokio::select! {
                biased;
                _ = shutdown_rx.cancelled() => {
                    Err(SchedulerError::QueryCancelled("cursor closed".to_owned()).into())
                }
                result = cursor.next(count, handler_args, formats, timeout_seconds, cancel_handle) => result,
            }
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
                if matches!(subscription_cursor.state, State::Invalid) {
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

#[cfg(test)]
mod cursor_lifecycle_tests {
    use std::collections::VecDeque;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use futures::StreamExt;
    use risingwave_batch::task::ShutdownToken;
    use risingwave_common::array::{DataChunk, DataChunkTestExt};
    use risingwave_common::catalog::TableId;
    use risingwave_common::error::BoxedError;
    use risingwave_sqlparser::parser::Parser;
    use tokio::sync::mpsc;
    use tokio_stream::wrappers::ReceiverStream;

    use super::{
        Cursor, CursorDataChunkStream, CursorManager, CursorQueryStream, CursorShutdownHandle,
        FetchCursorCancelHandle, FieldsManager, State, SubscriptionCursor,
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

    impl CursorDataChunkStream {
        /// Creates a local cursor stream backed by `chunk_rx`, without a real executor.
        /// The shutdown pair only satisfies the wrapper's constructor: the receiver is unused,
        /// and the sender has no executor to cancel.
        pub(crate) fn local_stream_without_executor_for_test(
            chunk_rx: mpsc::Receiver<Result<DataChunk, BoxedError>>,
        ) -> Self {
            let (shutdown_tx, _shutdown_rx) = ShutdownToken::new();
            Self::LocalDataChunk(Some(CursorQueryStream::local(
                ReceiverStream::new(chunk_rx),
                shutdown_tx,
            )))
        }
    }

    async fn add_pending_query_cursor(
        manager: &CursorManager,
        name: &str,
    ) -> (ShutdownToken, mpsc::Sender<Result<DataChunk, BoxedError>>) {
        let (chunk_tx, chunk_rx) = mpsc::channel(1);
        manager
            .add_query_cursor(
                name.to_owned(),
                CursorDataChunkStream::local_stream_without_executor_for_test(chunk_rx),
                vec![],
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
        (token, chunk_tx)
    }

    /// Creates a subscription cursor with a test-controlled pending stream for lifecycle tests.
    /// `add_subscription_cursor` does not accept an injected stream: it looks up the catalog and
    /// may start a snapshot query. Constructing `State::Fetch` directly avoids that setup while
    /// `insert_cursor` still exercises normal cursor and shutdown-sender registration.
    async fn add_pending_subscription_cursor(
        manager: &CursorManager,
        name: &str,
    ) -> (ShutdownToken, mpsc::Sender<Result<DataChunk, BoxedError>>) {
        let (chunk_tx, chunk_rx) = mpsc::channel(1);
        let cursor = SubscriptionCursor {
            shutdown_handle: CursorShutdownHandle::new(),
            cursor_name: name.to_owned(),
            subscription: Arc::new(SubscriptionCatalog {
                name: name.to_owned(),
                retention_seconds: 60,
                ..Default::default()
            }),
            dependent_table_id: TableId::new(1),
            cursor_need_drop_time: Instant::now() + Duration::from_secs(60),
            state: State::Fetch {
                from_snapshot: true,
                rw_timestamp: 0,
                chunk_stream: CursorDataChunkStream::local_stream_without_executor_for_test(
                    chunk_rx,
                ),
                remaining_rows: VecDeque::new(),
                expected_timestamp: None,
                init_query_timer: Instant::now(),
            },
            fields_manager: FieldsManager::new(&TableCatalog::default()),
            cursor_metrics: manager.cursor_metrics.clone(),
            last_fetch: Instant::now(),
            seek_pk_row: None,
        };
        let token = cursor.shutdown_handle.shutdown_token();
        let mut cursor_map = manager.cursor_map.lock().await;
        manager
            .insert_cursor(
                &mut cursor_map,
                name.to_owned(),
                Cursor::Subscription(cursor),
            )
            .unwrap();
        (token, chunk_tx)
    }

    /// Verifies that initiating shutdown signals all registered cursors and drops their streams
    /// before returning when the cursor map is unlocked.
    #[tokio::test]
    async fn test_cursor_shutdown_with_unlocked_map() {
        let manager = Arc::new(CursorManager::new(Arc::new(CursorMetrics::for_test())));
        let (first, first_tx) = add_pending_query_cursor(&manager, "first").await;
        let (second, second_tx) = add_pending_query_cursor(&manager, "second").await;

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
        let manager = Arc::new(CursorManager::new(Arc::new(CursorMetrics::for_test())));
        let (first, first_tx) = add_pending_query_cursor(&manager, "first").await;
        let (second, second_tx) = add_pending_query_cursor(&manager, "second").await;
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
        let manager = Arc::new(CursorManager::new(Arc::new(CursorMetrics::for_test())));
        let (handle, chunk_tx) = add_pending_query_cursor(&manager, "cursor").await;
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
                let (token, chunk_tx) = if is_subscription {
                    add_pending_subscription_cursor(&manager, "c").await
                } else {
                    add_pending_query_cursor(&manager, "c").await
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
        let manager = Arc::new(CursorManager::new(Arc::new(CursorMetrics::for_test())));
        let cursor_map = manager.cursor_map.lock().await;
        let (chunk_tx, chunk_rx) = mpsc::channel(1);
        let mut adding = Box::pin(manager.add_query_cursor(
            "late".to_owned(),
            CursorDataChunkStream::local_stream_without_executor_for_test(chunk_rx),
            vec![],
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
        let manager = Arc::new(CursorManager::new(Arc::new(CursorMetrics::for_test())));
        let (query, query_tx) = add_pending_query_cursor(&manager, "query").await;
        let (duplicate_tx, duplicate_rx) = mpsc::channel(1);

        assert!(
            manager
                .add_query_cursor(
                    "query".to_owned(),
                    CursorDataChunkStream::local_stream_without_executor_for_test(duplicate_rx),
                    vec![],
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
        let manager = Arc::new(CursorManager::new(Arc::new(CursorMetrics::for_test())));
        let (query, query_tx) = add_pending_query_cursor(&manager, "query").await;
        let (subscription, subscription_tx) =
            add_pending_subscription_cursor(&manager, "subscription").await;

        manager.remove_cursor("query").await.unwrap();

        assert!(query.is_cancelled());
        assert!(query_tx.is_closed());
        assert!(!subscription.is_cancelled());
        assert!(!subscription_tx.is_closed());
        let (replacement, replacement_tx) = add_pending_query_cursor(&manager, "query").await;
        assert!(!replacement.is_cancelled());
        assert!(!replacement_tx.is_closed());
    }

    /// Verifies that query-only cleanup signals query cursors and drops their streams while
    /// leaving subscription cursors unsignalled with their streams still open.
    #[tokio::test]
    async fn test_all_query_cursors_shutdown_preserves_subscription_cursors() {
        let manager = Arc::new(CursorManager::new(Arc::new(CursorMetrics::for_test())));
        let (query, query_tx) = add_pending_query_cursor(&manager, "query").await;
        let (subscription, subscription_tx) =
            add_pending_subscription_cursor(&manager, "subscription").await;

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
        let manager = Arc::new(CursorManager::new(Arc::new(CursorMetrics::for_test())));
        let (query, query_tx) = add_pending_query_cursor(&manager, "query").await;
        let (subscription, subscription_tx) =
            add_pending_subscription_cursor(&manager, "subscription").await;

        manager.remove_all_cursor().await;

        assert!(query.is_cancelled());
        assert!(query_tx.is_closed());
        assert!(subscription.is_cancelled());
        assert!(subscription_tx.is_closed());
        let (new_query, new_query_tx) = add_pending_query_cursor(&manager, "query").await;
        let (new_subscription, new_subscription_tx) =
            add_pending_subscription_cursor(&manager, "subscription").await;
        assert!(!new_query.is_cancelled());
        assert!(!new_query_tx.is_closed());
        assert!(!new_subscription.is_cancelled());
        assert!(!new_subscription_tx.is_closed());
    }

    /// Verifies that unfinished local cursor shutdown via CLOSE signals its executor token and drops
    /// its output stream without signaling the ordinary local query. A query cursor converts its
    /// chunk stream to a row stream only when the first FETCH is invoked, so shutdown must work
    /// with either representation.
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
            let mut stream = CursorDataChunkStream::LocalDataChunk(Some(stream));
            if initialize_rows {
                stream.init_row_stream(&vec![], &vec![], session.clone());
            }
            let manager = session.get_cursor_manager();
            manager
                .add_query_cursor("cursor".to_owned(), stream, vec![])
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
    /// receives no cancellation request. A query cursor converts its chunk stream to a row stream
    /// only when the first FETCH is invoked, so shutdown must work with either representation.
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
            let mut chunk_stream = CursorDataChunkStream::DistributedDataChunk(Some(stream));
            if initialize_rows {
                chunk_stream.init_row_stream(&vec![], &vec![], session.clone());
                assert!(futures::poll!(std::pin::pin!(chunk_stream.next())).is_pending());
            }
            let cursors = session.get_cursor_manager();
            cursors
                .add_query_cursor("cursor".to_owned(), chunk_stream, vec![])
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
