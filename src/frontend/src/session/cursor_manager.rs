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
use std::collections::{HashMap, HashSet, VecDeque};
use std::rc::Rc;
use std::sync::Arc;
use std::time::Instant;

use anyhow::anyhow;
use bytes::Bytes;
use fixedbitset::FixedBitSet;
use futures::StreamExt;
use pgwire::pg_field_descriptor::PgFieldDescriptor;
use pgwire::pg_response::StatementType;
use pgwire::types::{Format, Row};
use risingwave_common::catalog::Field;
use risingwave_common::error::BoxedError;
use risingwave_common::session_config::QueryMode;
use risingwave_common::types::DataType;
use risingwave_sqlparser::ast::{Ident, ObjectName, Statement};

use super::SessionImpl;
use crate::catalog::subscription_catalog::SubscriptionCatalog;
use crate::catalog::TableId;
use crate::error::{ErrorCode, Result};
use crate::handler::declare_cursor::{
    create_chunk_stream_for_cursor, create_stream_for_cursor_stmt,
};
use crate::handler::query::{gen_batch_plan_fragmenter, BatchQueryPlanResult};
use crate::handler::util::{
    convert_logstore_u64_to_unix_millis, gen_query_from_table_name, pg_value_format, to_pg_field,
    DataChunkToRowSetAdapter, StaticSessionData,
};
use crate::handler::HandlerArgs;
use crate::monitor::{CursorMetrics, PeriodicCursorMetrics};
use crate::optimizer::plan_node::{generic, BatchLogSeqScan};
use crate::optimizer::property::{Order, RequiredDist};
use crate::optimizer::PlanRoot;
use crate::scheduler::{DistributedQueryStream, LocalQueryStream};
use crate::{OptimizerContext, OptimizerContextRef, PgResponseStream, PlanRef, TableCatalog};

pub enum CursorDataChunkStream {
    LocalDataChunk(Option<LocalQueryStream>),
    DistributedDataChunk(Option<DistributedQueryStream>),
    PgResponse(PgResponseStream),
}

impl CursorDataChunkStream {
    pub fn init_row_stream(
        &mut self,
        fields: &Vec<Field>,
        formats: &Vec<Format>,
        session: Arc<SessionImpl>,
    ) {
        let columns_type = fields.iter().map(|f| f.data_type().clone()).collect();
        match self {
            CursorDataChunkStream::LocalDataChunk(data_chunk) => {
                let data_chunk = mem::take(data_chunk).unwrap();
                let row_stream = PgResponseStream::LocalQuery(DataChunkToRowSetAdapter::new(
                    data_chunk,
                    columns_type,
                    formats.clone(),
                    session,
                ));
                *self = CursorDataChunkStream::PgResponse(row_stream);
            }
            CursorDataChunkStream::DistributedDataChunk(data_chunk) => {
                let data_chunk = mem::take(data_chunk).unwrap();
                let row_stream = PgResponseStream::DistributedQuery(DataChunkToRowSetAdapter::new(
                    data_chunk,
                    columns_type,
                    formats.clone(),
                    session,
                ));
                *self = CursorDataChunkStream::PgResponse(row_stream);
            }
            _ => {}
        }
    }

    pub async fn next(&mut self) -> Result<Option<std::result::Result<Vec<Row>, BoxedError>>> {
        match self {
            CursorDataChunkStream::PgResponse(row_stream) => Ok(row_stream.next().await),
            _ => Err(ErrorCode::InternalError(
                "Only 'CursorDataChunkStream' can call next and return rows".to_string(),
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
    pub async fn next(
        &mut self,
        count: u32,
        handle_args: HandlerArgs,
        formats: &Vec<Format>,
    ) -> Result<(Vec<Row>, Vec<PgFieldDescriptor>)> {
        match self {
            Cursor::Subscription(cursor) => cursor
                .next(count, handle_args, formats)
                .await
                .inspect_err(|_| cursor.cursor_metrics.subscription_cursor_error_count.inc()),
            Cursor::Query(cursor) => cursor.next(count, formats, handle_args).await,
        }
    }

    pub fn get_fields(&mut self) -> Vec<Field> {
        match self {
            Cursor::Subscription(cursor) => cursor.fields.clone(),
            Cursor::Query(cursor) => cursor.fields.clone(),
        }
    }
}

pub struct QueryCursor {
    chunk_stream: CursorDataChunkStream,
    fields: Vec<Field>,
    remaining_rows: VecDeque<Row>,
}

impl QueryCursor {
    pub fn new(chunk_stream: CursorDataChunkStream, fields: Vec<Field>) -> Result<Self> {
        Ok(Self {
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
        handle_args: HandlerArgs,
    ) -> Result<(Vec<Row>, Vec<PgFieldDescriptor>)> {
        // `FETCH NEXT` is equivalent to `FETCH 1`.
        // min with 100 to avoid allocating too many memory at once.
        let session = handle_args.session;
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

pub struct SubscriptionCursor {
    cursor_name: String,
    subscription: Arc<SubscriptionCatalog>,
    dependent_table_id: TableId,
    cursor_need_drop_time: Instant,
    state: State,
    // fields will be set in the table's catalog when the cursor is created,
    // and will be reset each time it is created chunk_stream, this is to avoid changes in the catalog due to alter.
    fields: Vec<Field>,
    cursor_metrics: Arc<CursorMetrics>,
    last_fetch: Instant,
}

impl SubscriptionCursor {
    pub async fn new(
        cursor_name: String,
        start_timestamp: Option<u64>,
        subscription: Arc<SubscriptionCatalog>,
        dependent_table_id: TableId,
        handle_args: &HandlerArgs,
        cursor_metrics: Arc<CursorMetrics>,
    ) -> Result<Self> {
        let (state, fields) = if let Some(start_timestamp) = start_timestamp {
            let table_catalog = handle_args.session.get_table_by_id(&dependent_table_id)?;
            let fields = table_catalog
                .columns
                .iter()
                .filter(|c| !c.is_hidden)
                .map(|c| Field::with_name(c.data_type().clone(), c.name()))
                .collect();
            let fields = Self::build_desc(fields, true);
            (
                State::InitLogStoreQuery {
                    seek_timestamp: start_timestamp,
                    expected_timestamp: None,
                },
                fields,
            )
        } else {
            // The query stream needs to initiated on cursor creation to make sure
            // future fetch on the cursor starts from the snapshot when the cursor is declared.
            //
            // TODO: is this the right behavior? Should we delay the query stream initiation till the first fetch?
            let (chunk_stream, fields, init_query_timer) =
                Self::initiate_query(None, &dependent_table_id, handle_args.clone()).await?;
            let pinned_epoch = handle_args
                .session
                .env
                .hummock_snapshot_manager
                .acquire()
                .version()
                .state_table_info
                .info()
                .get(&dependent_table_id)
                .ok_or_else(|| anyhow!("dependent_table_id {dependent_table_id} not exists"))?
                .committed_epoch;
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
                fields,
            )
        };

        let cursor_need_drop_time =
            Instant::now() + Duration::from_secs(subscription.retention_seconds);
        Ok(Self {
            cursor_name,
            subscription,
            dependent_table_id,
            cursor_need_drop_time,
            state,
            fields,
            cursor_metrics,
            last_fetch: Instant::now(),
        })
    }

    async fn next_row(
        &mut self,
        handle_args: &HandlerArgs,
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
                        &self.dependent_table_id,
                        *expected_timestamp,
                        handle_args.clone(),
                        &self.subscription,
                    )
                    .await
                    {
                        Ok((Some(rw_timestamp), expected_timestamp)) => {
                            let (mut chunk_stream, fields, init_query_timer) =
                                Self::initiate_query(
                                    Some(rw_timestamp),
                                    &self.dependent_table_id,
                                    handle_args.clone(),
                                )
                                .await?;
                            Self::init_row_stream(
                                &mut chunk_stream,
                                formats,
                                &from_snapshot,
                                &fields,
                                handle_args.session.clone(),
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
                            if self.fields.ne(&fields) {
                                self.fields = fields;
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
                        timezone: handle_args.session.config().timezone(),
                    };
                    let from_snapshot = *from_snapshot;
                    let rw_timestamp = *rw_timestamp;

                    // Try refill remaining rows
                    Self::try_refill_remaining_rows(chunk_stream, remaining_rows).await?;

                    if let Some(row) = remaining_rows.pop_front() {
                        // 1. Fetch the next row
                        let new_row = row.take();
                        if from_snapshot {
                            return Ok(Some(Row::new(Self::build_row(
                                new_row,
                                None,
                                formats,
                                &session_data,
                            )?)));
                        } else {
                            return Ok(Some(Row::new(Self::build_row(
                                new_row,
                                Some(rw_timestamp),
                                formats,
                                &session_data,
                            )?)));
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
                            .to_string(),
                    )
                    .into());
                }
            }
        }
    }

    pub async fn next(
        &mut self,
        count: u32,
        handle_args: HandlerArgs,
        formats: &Vec<Format>,
    ) -> Result<(Vec<Row>, Vec<PgFieldDescriptor>)> {
        if Instant::now() > self.cursor_need_drop_time {
            return Err(ErrorCode::InternalError(
                "The cursor has exceeded its maximum lifetime, please recreate it (close then declare cursor).".to_string(),
            )
            .into());
        }

        let mut ans = Vec::with_capacity(std::cmp::min(100, count) as usize);
        let mut cur = 0;
        let desc = self.fields.iter().map(to_pg_field).collect();
        if let State::Fetch {
            from_snapshot,
            chunk_stream,
            ..
        } = &mut self.state
        {
            Self::init_row_stream(
                chunk_stream,
                formats,
                from_snapshot,
                &self.fields,
                handle_args.session.clone(),
            );
        }
        while cur < count {
            let fetch_cursor_timer = Instant::now();
            let row = self.next_row(&handle_args, formats).await?;
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
                    break;
                }
            }
        }
        self.last_fetch = Instant::now();

        Ok((ans, desc))
    }

    async fn get_next_rw_timestamp(
        seek_timestamp: u64,
        table_id: &TableId,
        expected_timestamp: Option<u64>,
        handle_args: HandlerArgs,
        dependent_subscription: &SubscriptionCatalog,
    ) -> Result<(Option<u64>, Option<u64>)> {
        let session = handle_args.session;
        // Test subscription existence
        session.get_subscription_by_schema_id_name(
            dependent_subscription.schema_id,
            &dependent_subscription.name,
        )?;

        // The epoch here must be pulled every time, otherwise there will be cache consistency issues
        let new_epochs = session
            .list_change_log_epochs(table_id.table_id(), seek_timestamp, 2)
            .await?;
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

    async fn initiate_query(
        rw_timestamp: Option<u64>,
        dependent_table_id: &TableId,
        handle_args: HandlerArgs,
    ) -> Result<(CursorDataChunkStream, Vec<Field>, Instant)> {
        let session = handle_args.clone().session;
        let table_catalog = session.get_table_by_id(dependent_table_id)?;
        let init_query_timer = Instant::now();
        let (chunk_stream, fields) = if let Some(rw_timestamp) = rw_timestamp {
            let context = OptimizerContext::from_handler_args(handle_args);
            let plan_fragmenter_result = gen_batch_plan_fragmenter(
                &session,
                Self::create_batch_plan_for_cursor(
                    &table_catalog,
                    &session,
                    context.into(),
                    rw_timestamp,
                    rw_timestamp,
                )?,
            )?;
            create_chunk_stream_for_cursor(session, plan_fragmenter_result).await?
        } else {
            let subscription_from_table_name =
                ObjectName(vec![Ident::from(table_catalog.name.as_ref())]);
            let query_stmt = Statement::Query(Box::new(gen_query_from_table_name(
                subscription_from_table_name,
            )));
            create_stream_for_cursor_stmt(handle_args, query_stmt).await?
        };
        Ok((
            chunk_stream,
            Self::build_desc(fields, rw_timestamp.is_none()),
            init_query_timer,
        ))
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
    ) -> Result<Vec<Option<Bytes>>> {
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
        Ok(row)
    }

    pub fn build_desc(mut descs: Vec<Field>, from_snapshot: bool) -> Vec<Field> {
        if from_snapshot {
            descs.push(Field::with_name(DataType::Varchar, "op"));
        }
        descs.push(Field::with_name(DataType::Int64, "rw_timestamp"));
        descs
    }

    pub fn create_batch_plan_for_cursor(
        table_catalog: &TableCatalog,
        session: &SessionImpl,
        context: OptimizerContextRef,
        old_epoch: u64,
        new_epoch: u64,
    ) -> Result<BatchQueryPlanResult> {
        let out_col_idx = table_catalog
            .columns
            .iter()
            .enumerate()
            .filter(|(_, v)| !v.is_hidden)
            .map(|(i, _)| i)
            .collect::<Vec<_>>();
        let core = generic::LogScan::new(
            table_catalog.name.clone(),
            out_col_idx,
            Rc::new(table_catalog.table_desc()),
            context,
            old_epoch,
            new_epoch,
        );
        let batch_log_seq_scan = BatchLogSeqScan::new(core);
        let schema = batch_log_seq_scan
            .core()
            .schema_without_table_name()
            .clone();
        let out_fields = FixedBitSet::from_iter(0..schema.len());
        let out_names = batch_log_seq_scan.core().column_names();
        // Here we just need a plan_root to call the method, only out_fields and out_names will be used
        let plan_root = PlanRoot::new_with_batch_plan(
            PlanRef::from(batch_log_seq_scan.clone()),
            RequiredDist::single(),
            Order::default(),
            out_fields,
            out_names,
        );
        let (batch_log_seq_scan, query_mode) = match session.config().query_mode() {
            QueryMode::Auto => (plan_root.gen_batch_local_plan()?, QueryMode::Local),
            QueryMode::Local => (plan_root.gen_batch_local_plan()?, QueryMode::Local),
            QueryMode::Distributed => (
                plan_root.gen_batch_distributed_plan()?,
                QueryMode::Distributed,
            ),
        };
        Ok(BatchQueryPlanResult {
            plan: batch_log_seq_scan,
            query_mode,
            schema,
            stmt_type: StatementType::SELECT,
            dependent_relations: table_catalog.dependent_relations.clone(),
            read_storage_tables: HashSet::from_iter([table_catalog.id]),
        })
    }

    // In the beginning (declare cur), we will give it an empty formats,
    // this formats is not a real, when we fetch, We fill it with the formats returned from the pg client.
    pub fn init_row_stream(
        chunk_stream: &mut CursorDataChunkStream,
        formats: &Vec<Format>,
        from_snapshot: &bool,
        fields: &Vec<Field>,
        session: Arc<SessionImpl>,
    ) {
        let mut formats = formats.clone();
        let mut fields = fields.clone();
        formats.pop();
        fields.pop();
        if *from_snapshot {
            formats.pop();
            fields.pop();
        }
        chunk_stream.init_row_stream(&fields, &formats, session);
    }
}

pub struct CursorManager {
    cursor_map: tokio::sync::Mutex<HashMap<String, Cursor>>,
    cursor_metrics: Arc<CursorMetrics>,
}

impl CursorManager {
    pub fn new(cursor_metrics: Arc<CursorMetrics>) -> Self {
        Self {
            cursor_map: tokio::sync::Mutex::new(HashMap::new()),
            cursor_metrics,
        }
    }

    pub async fn add_subscription_cursor(
        &self,
        cursor_name: String,
        start_timestamp: Option<u64>,
        dependent_table_id: TableId,
        subscription: Arc<SubscriptionCatalog>,
        handle_args: &HandlerArgs,
    ) -> Result<()> {
        let create_cursor_timer = Instant::now();
        let subscription_name = subscription.name.clone();
        let cursor = SubscriptionCursor::new(
            cursor_name.clone(),
            start_timestamp,
            subscription,
            dependent_table_id,
            handle_args,
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
                && matches!(cursor.state, State::Invalid)
            {
                false
            } else {
                true
            }
        });

        cursor_map
            .try_insert(cursor.cursor_name.clone(), Cursor::Subscription(cursor))
            .map_err(|_| {
                ErrorCode::CatalogError(format!("cursor `{}` already exists", cursor_name).into())
            })?;
        Ok(())
    }

    pub async fn add_query_cursor(
        &self,
        cursor_name: ObjectName,
        chunk_stream: CursorDataChunkStream,
        fields: Vec<Field>,
    ) -> Result<()> {
        let cursor = QueryCursor::new(chunk_stream, fields)?;
        self.cursor_map
            .lock()
            .await
            .try_insert(cursor_name.to_string(), Cursor::Query(cursor))
            .map_err(|_| {
                ErrorCode::CatalogError(format!("cursor `{}` already exists", cursor_name).into())
            })?;

        Ok(())
    }

    pub async fn remove_cursor(&self, cursor_name: String) -> Result<()> {
        self.cursor_map
            .lock()
            .await
            .remove(&cursor_name)
            .ok_or_else(|| {
                ErrorCode::CatalogError(format!("cursor `{}` don't exists", cursor_name).into())
            })?;
        Ok(())
    }

    pub async fn remove_all_cursor(&self) {
        self.cursor_map.lock().await.clear();
    }

    pub async fn remove_all_query_cursor(&self) {
        self.cursor_map
            .lock()
            .await
            .retain(|_, v| matches!(v, Cursor::Subscription(_)));
    }

    pub async fn get_rows_with_cursor(
        &self,
        cursor_name: String,
        count: u32,
        handle_args: HandlerArgs,
        formats: &Vec<Format>,
    ) -> Result<(Vec<Row>, Vec<PgFieldDescriptor>)> {
        if let Some(cursor) = self.cursor_map.lock().await.get_mut(&cursor_name) {
            cursor.next(count, handle_args, formats).await
        } else {
            Err(ErrorCode::ItemNotFound(format!("Cannot find cursor `{}`", cursor_name)).into())
        }
    }

    pub async fn get_fields_with_cursor(&self, cursor_name: String) -> Result<Vec<Field>> {
        if let Some(cursor) = self.cursor_map.lock().await.get_mut(&cursor_name) {
            Ok(cursor.get_fields())
        } else {
            Err(ErrorCode::ItemNotFound(format!("Cannot find cursor `{}`", cursor_name)).into())
        }
    }

    pub async fn get_periodic_cursor_metrics(&self) -> PeriodicCursorMetrics {
        let mut subsription_cursor_nums = 0;
        let mut invalid_subsription_cursor_nums = 0;
        let mut subscription_cursor_last_fetch_duration = HashMap::new();
        for (_, cursor) in self.cursor_map.lock().await.iter() {
            if let Cursor::Subscription(subscription_cursor) = cursor {
                subsription_cursor_nums += 1;
                if matches!(subscription_cursor.state, State::Invalid) {
                    invalid_subsription_cursor_nums += 1;
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
            subsription_cursor_nums,
            invalid_subsription_cursor_nums,
            subscription_cursor_last_fetch_duration,
        }
    }

    pub async fn get_all_query_cursors(&self) -> (Vec<Row>, Vec<PgFieldDescriptor>) {
        let cursor_names = self
            .cursor_map
            .lock()
            .await
            .iter()
            .filter_map(|(currsor_name, cursor)| {
                if let Cursor::Query(_cursor) = cursor {
                    let cursor_name = vec![Some(Bytes::from(currsor_name.clone().into_bytes()))];
                    Some(Row::new(cursor_name))
                } else {
                    None
                }
            })
            .collect();
        (
            cursor_names,
            vec![PgFieldDescriptor::new(
                "Name".to_string(),
                DataType::Varchar.to_oid(),
                DataType::Varchar.type_len(),
            )],
        )
    }

    pub async fn get_all_subscription_cursors(&self) -> (Vec<Row>, Vec<PgFieldDescriptor>) {
        let cursors = self
            .cursor_map
            .lock()
            .await
            .iter()
            .filter_map(|(cursor_name, cursor)| {
                if let Cursor::Subscription(cursor) = cursor {
                    let cursors = vec![
                        Some(Bytes::from(cursor_name.clone().into_bytes())),
                        Some(Bytes::from(cursor.subscription.name.clone().into_bytes())),
                    ];
                    Some(Row::new(cursors))
                } else {
                    None
                }
            })
            .collect();
        (
            cursors,
            vec![
                PgFieldDescriptor::new(
                    "Name".to_string(),
                    DataType::Varchar.to_oid(),
                    DataType::Varchar.type_len(),
                ),
                PgFieldDescriptor::new(
                    "SubscriptionName".to_string(),
                    DataType::Varchar.to_oid(),
                    DataType::Varchar.type_len(),
                ),
            ],
        )
    }
}
