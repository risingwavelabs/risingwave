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
use std::collections::{HashMap, VecDeque};
use std::rc::Rc;
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use fixedbitset::FixedBitSet;
use futures::StreamExt;
use pgwire::pg_field_descriptor::PgFieldDescriptor;
use pgwire::pg_response::StatementType;
use pgwire::types::Row;
use risingwave_common::session_config::QueryMode;
use risingwave_common::types::DataType;
use risingwave_sqlparser::ast::{Ident, ObjectName, Statement};

use super::SessionImpl;
use crate::catalog::subscription_catalog::SubscriptionCatalog;
use crate::error::{ErrorCode, Result};
use crate::handler::declare_cursor::create_stream_for_cursor_stmt;
use crate::handler::query::{create_stream, gen_batch_plan_fragmenter, BatchQueryPlanResult};
use crate::handler::util::{convert_logstore_u64_to_unix_millis, gen_query_from_table_name};
use crate::handler::HandlerArgs;
use crate::optimizer::plan_node::{generic, BatchLogSeqScan};
use crate::optimizer::property::{Order, RequiredDist};
use crate::optimizer::PlanRoot;
use crate::{OptimizerContext, OptimizerContextRef, PgResponseStream, PlanRef, TableCatalog};

pub enum Cursor {
    Subscription(SubscriptionCursor),
    Query(QueryCursor),
}
impl Cursor {
    pub async fn next(
        &mut self,
        count: u32,
        handle_args: HandlerArgs,
    ) -> Result<(Vec<Row>, Vec<PgFieldDescriptor>)> {
        match self {
            Cursor::Subscription(cursor) => cursor.next(count, handle_args).await,
            Cursor::Query(cursor) => cursor.next(count).await,
        }
    }
}

pub struct QueryCursor {
    row_stream: PgResponseStream,
    pg_descs: Vec<PgFieldDescriptor>,
    remaining_rows: VecDeque<Row>,
}

impl QueryCursor {
    pub fn new(row_stream: PgResponseStream, pg_descs: Vec<PgFieldDescriptor>) -> Result<Self> {
        Ok(Self {
            row_stream,
            pg_descs,
            remaining_rows: VecDeque::<Row>::new(),
        })
    }

    pub async fn next_once(&mut self) -> Result<Option<Row>> {
        while self.remaining_rows.is_empty() {
            let rows = self.row_stream.next().await;
            let rows = match rows {
                None => return Ok(None),
                Some(row) => row?,
            };
            self.remaining_rows = rows.into_iter().collect();
        }
        let row = self.remaining_rows.pop_front().unwrap();
        Ok(Some(row))
    }

    pub async fn next(&mut self, count: u32) -> Result<(Vec<Row>, Vec<PgFieldDescriptor>)> {
        // `FETCH NEXT` is equivalent to `FETCH 1`.
        // min with 100 to avoid allocating too many memory at once.
        let mut ans = Vec::with_capacity(std::cmp::min(100, count) as usize);
        let mut cur = 0;
        while cur < count
            && let Some(row) = self.next_once().await?
        {
            cur += 1;
            ans.push(row);
        }
        Ok((ans, self.pg_descs.clone()))
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
        row_stream: PgResponseStream,

        // The pg descs to from the batch query read.
        // It is returned from the batch execution.
        pg_descs: Vec<PgFieldDescriptor>,

        // A cache to store the remaining rows from the row stream.
        remaining_rows: VecDeque<Row>,

        expected_timestamp: Option<u64>,
    },
    Invalid,
}

pub struct SubscriptionCursor {
    cursor_name: String,
    subscription: Arc<SubscriptionCatalog>,
    table: Arc<TableCatalog>,
    cursor_need_drop_time: Instant,
    state: State,
}

impl SubscriptionCursor {
    pub async fn new(
        cursor_name: String,
        start_timestamp: Option<u64>,
        subscription: Arc<SubscriptionCatalog>,
        table: Arc<TableCatalog>,
        handle_args: &HandlerArgs,
    ) -> Result<Self> {
        let state = if let Some(start_timestamp) = start_timestamp {
            State::InitLogStoreQuery {
                seek_timestamp: start_timestamp,
                expected_timestamp: None,
            }
        } else {
            // The query stream needs to initiated on cursor creation to make sure
            // future fetch on the cursor starts from the snapshot when the cursor is declared.
            //
            // TODO: is this the right behavior? Should we delay the query stream initiation till the first fetch?
            let (row_stream, pg_descs) =
                Self::initiate_query(None, &table, handle_args.clone()).await?;
            let pinned_epoch = handle_args
                .session
                .get_pinned_snapshot()
                .ok_or_else(|| {
                    ErrorCode::InternalError("Fetch Cursor can't find snapshot epoch".to_string())
                })?
                .epoch_with_frontend_pinned()
                .ok_or_else(|| {
                    ErrorCode::InternalError(
                        "Fetch Cursor can't support setting an epoch".to_string(),
                    )
                })?
                .0;
            let start_timestamp = pinned_epoch;

            State::Fetch {
                from_snapshot: true,
                rw_timestamp: start_timestamp,
                row_stream,
                pg_descs,
                remaining_rows: VecDeque::new(),
                expected_timestamp: None,
            }
        };

        let cursor_need_drop_time =
            Instant::now() + Duration::from_secs(subscription.retention_seconds);
        Ok(Self {
            cursor_name,
            subscription,
            table,
            cursor_need_drop_time,
            state,
        })
    }

    pub async fn next_row(
        &mut self,
        handle_args: HandlerArgs,
    ) -> Result<(Option<Row>, Vec<PgFieldDescriptor>)> {
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
                        self.table.id.table_id,
                        *expected_timestamp,
                        handle_args.clone(),
                    )
                    .await
                    {
                        Ok((Some(rw_timestamp), expected_timestamp)) => {
                            let (mut row_stream, pg_descs) = Self::initiate_query(
                                Some(rw_timestamp),
                                &self.table,
                                handle_args.clone(),
                            )
                            .await?;
                            self.cursor_need_drop_time = Instant::now()
                                + Duration::from_secs(self.subscription.retention_seconds);
                            let mut remaining_rows = VecDeque::new();
                            Self::try_refill_remaining_rows(&mut row_stream, &mut remaining_rows)
                                .await?;
                            // Transition to the Fetch state
                            self.state = State::Fetch {
                                from_snapshot,
                                rw_timestamp,
                                row_stream,
                                pg_descs,
                                remaining_rows,
                                expected_timestamp,
                            };
                        }
                        Ok((None, _)) => return Ok((None, vec![])),
                        Err(e) => {
                            self.state = State::Invalid;
                            return Err(e);
                        }
                    }
                }
                State::Fetch {
                    from_snapshot,
                    rw_timestamp,
                    row_stream,
                    pg_descs,
                    remaining_rows,
                    expected_timestamp,
                } => {
                    let from_snapshot = *from_snapshot;
                    let rw_timestamp = *rw_timestamp;

                    // Try refill remaining rows
                    Self::try_refill_remaining_rows(row_stream, remaining_rows).await?;

                    if let Some(row) = remaining_rows.pop_front() {
                        // 1. Fetch the next row
                        let new_row = row.take();
                        if from_snapshot {
                            return Ok((
                                Some(Row::new(Self::build_row(new_row, None)?)),
                                pg_descs.clone(),
                            ));
                        } else {
                            return Ok((
                                Some(Row::new(Self::build_row(new_row, Some(rw_timestamp))?)),
                                pg_descs.clone(),
                            ));
                        }
                    } else {
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
    ) -> Result<(Vec<Row>, Vec<PgFieldDescriptor>)> {
        if Instant::now() > self.cursor_need_drop_time {
            return Err(ErrorCode::InternalError(
                "The cursor has exceeded its maximum lifetime, please recreate it (close then declare cursor).".to_string(),
            )
            .into());
        }
        // `FETCH NEXT` is equivalent to `FETCH 1`.
        if count != 1 {
            Err(crate::error::ErrorCode::InternalError(
                "FETCH count with subscription is not supported".to_string(),
            )
            .into())
        } else {
            let (row, pg_descs) = self.next_row(handle_args).await?;
            if let Some(row) = row {
                Ok((vec![row], pg_descs))
            } else {
                Ok((vec![], pg_descs))
            }
        }
    }

    async fn get_next_rw_timestamp(
        seek_timestamp: u64,
        table_id: u32,
        expected_timestamp: Option<u64>,
        handle_args: HandlerArgs,
    ) -> Result<(Option<u64>, Option<u64>)> {
        // The epoch here must be pulled every time, otherwise there will be cache consistency issues
        let new_epochs = handle_args
            .session
            .catalog_writer()?
            .list_change_log_epochs(table_id, seek_timestamp, 2)
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
        table_catalog: &TableCatalog,
        handle_args: HandlerArgs,
    ) -> Result<(PgResponseStream, Vec<PgFieldDescriptor>)> {
        let (row_stream, pg_descs) = if let Some(rw_timestamp) = rw_timestamp {
            let context = OptimizerContext::from_handler_args(handle_args.clone());
            let session = handle_args.session;
            let plan_fragmenter_result = gen_batch_plan_fragmenter(
                &session,
                Self::create_batch_plan_for_cursor(
                    table_catalog,
                    &session,
                    context.into(),
                    rw_timestamp,
                    rw_timestamp,
                )?,
            )?;
            create_stream(session, plan_fragmenter_result, vec![]).await?
        } else {
            let subscription_from_table_name =
                ObjectName(vec![Ident::from(table_catalog.name.as_ref())]);
            let query_stmt = Statement::Query(Box::new(gen_query_from_table_name(
                subscription_from_table_name,
            )));
            create_stream_for_cursor_stmt(handle_args, query_stmt).await?
        };
        Ok((
            row_stream,
            Self::build_desc(pg_descs, rw_timestamp.is_none()),
        ))
    }

    async fn try_refill_remaining_rows(
        row_stream: &mut PgResponseStream,
        remaining_rows: &mut VecDeque<Row>,
    ) -> Result<()> {
        if remaining_rows.is_empty()
            && let Some(row_set) = row_stream.next().await
        {
            remaining_rows.extend(row_set?);
        }
        Ok(())
    }

    pub fn build_row(
        mut row: Vec<Option<Bytes>>,
        rw_timestamp: Option<u64>,
    ) -> Result<Vec<Option<Bytes>>> {
        let new_row = if let Some(rw_timestamp) = rw_timestamp {
            vec![Some(Bytes::from(
                convert_logstore_u64_to_unix_millis(rw_timestamp).to_string(),
            ))]
        } else {
            vec![Some(Bytes::from(1i16.to_string())), None]
        };
        row.extend(new_row);
        Ok(row)
    }

    pub fn build_desc(
        mut descs: Vec<PgFieldDescriptor>,
        from_snapshot: bool,
    ) -> Vec<PgFieldDescriptor> {
        if from_snapshot {
            descs.push(PgFieldDescriptor::new(
                "op".to_owned(),
                DataType::Int16.to_oid(),
                DataType::Int16.type_len(),
            ));
        }
        descs.push(PgFieldDescriptor::new(
            "rw_timestamp".to_owned(),
            DataType::Int64.to_oid(),
            DataType::Int64.type_len(),
        ));
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
        let out_fields = FixedBitSet::from_iter(0..batch_log_seq_scan.core().schema().len());
        let out_names = batch_log_seq_scan.core().column_names();
        // Here we just need a plan_root to call the method, only out_fields and out_names will be used
        let plan_root = PlanRoot::new_with_batch_plan(
            PlanRef::from(batch_log_seq_scan.clone()),
            RequiredDist::single(),
            Order::default(),
            out_fields,
            out_names,
        );
        let schema = batch_log_seq_scan.core().schema().clone();
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
        })
    }
}

#[derive(Default)]
pub struct CursorManager {
    cursor_map: tokio::sync::Mutex<HashMap<String, Cursor>>,
}

impl CursorManager {
    pub async fn add_subscription_cursor(
        &self,
        cursor_name: String,
        start_timestamp: Option<u64>,
        subscription: Arc<SubscriptionCatalog>,
        table: Arc<TableCatalog>,
        handle_args: &HandlerArgs,
    ) -> Result<()> {
        let cursor = SubscriptionCursor::new(
            cursor_name.clone(),
            start_timestamp,
            subscription,
            table,
            handle_args,
        )
        .await?;
        self.cursor_map
            .lock()
            .await
            .try_insert(cursor.cursor_name.clone(), Cursor::Subscription(cursor))
            .map_err(|_| {
                ErrorCode::CatalogError(format!("cursor `{}` already exists", cursor_name).into())
            })?;
        Ok(())
    }

    pub async fn add_query_cursor(
        &self,
        cursor_name: ObjectName,
        row_stream: PgResponseStream,
        pg_descs: Vec<PgFieldDescriptor>,
    ) -> Result<()> {
        let cursor = QueryCursor::new(row_stream, pg_descs)?;
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
    ) -> Result<(Vec<Row>, Vec<PgFieldDescriptor>)> {
        if let Some(cursor) = self.cursor_map.lock().await.get_mut(&cursor_name) {
            cursor.next(count, handle_args).await
        } else {
            Err(ErrorCode::ItemNotFound(format!("Cannot find cursor `{}`", cursor_name)).into())
        }
    }
}
