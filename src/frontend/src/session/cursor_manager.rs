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

use core::ops::Index;
use core::time::Duration;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use futures::StreamExt;
use itertools::Itertools;
use pgwire::pg_field_descriptor::PgFieldDescriptor;
use pgwire::types::Row;
use risingwave_common::types::DataType;
use risingwave_sqlparser::ast::{Ident, ObjectName, Statement};

use crate::catalog::subscription_catalog::SubscriptionCatalog;
use crate::error::{ErrorCode, Result};
use crate::handler::declare_cursor::create_stream_for_cursor;
use crate::handler::util::{
    convert_epoch_to_logstore_i64, convert_logstore_i64_to_unix_millis,
    gen_query_from_logstore_ge_rw_timestamp, gen_query_from_table_name,
};
use crate::handler::HandlerArgs;
use crate::PgResponseStream;

pub const KV_LOG_STORE_EPOCH: &str = "kv_log_store_epoch";
const KV_LOG_STORE_ROW_OP: &str = "kv_log_store_row_op";
pub const KV_LOG_STORE_SEQ_ID: &str = "kv_log_store_seq_id";
pub const KV_LOG_STORE_VNODE: &str = "kv_log_store_vnode";

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
        seek_timestamp: i64,

        // If specified, the expected_timestamp must be an exact match for the next rw_timestamp.
        expected_timestamp: Option<i64>,
    },
    Fetch {
        // Whether the query is reading from snapshot
        // true: read from the upstream table snapshot
        // false: read from subscription logstore
        from_snapshot: bool,

        // The rw_timestamp used to initiate the query to read from subscription logstore.
        rw_timestamp: i64,

        // The row stream to from the batch query read.
        // It is returned from the batch execution.
        row_stream: PgResponseStream,

        // The pg descs to from the batch query read.
        // It is returned from the batch execution.
        pg_descs: Vec<PgFieldDescriptor>,

        // A cache to store the remaining rows from the row stream.
        remaining_rows: VecDeque<Row>,
    },
    Invalid,
}

pub struct SubscriptionCursor {
    cursor_name: String,
    subscription: Arc<SubscriptionCatalog>,
    cursor_need_drop_time: Instant,
    state: State,
}

impl SubscriptionCursor {
    pub async fn new(
        cursor_name: String,
        start_timestamp: Option<i64>,
        subscription: Arc<SubscriptionCatalog>,
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
                Self::initiate_query(None, &subscription, handle_args.clone()).await?;
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
            let start_timestamp = convert_epoch_to_logstore_i64(pinned_epoch);

            State::Fetch {
                from_snapshot: true,
                rw_timestamp: start_timestamp,
                row_stream,
                pg_descs,
                remaining_rows: VecDeque::new(),
            }
        };

        let cursor_need_drop_time =
            Instant::now() + Duration::from_secs(subscription.get_retention_seconds()?);
        Ok(Self {
            cursor_name,
            subscription,
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
                    let (mut row_stream, pg_descs) = Self::initiate_query(
                        Some(*seek_timestamp),
                        &self.subscription,
                        handle_args.clone(),
                    )
                    .await?;
                    self.cursor_need_drop_time = Instant::now()
                        + Duration::from_secs(self.subscription.get_retention_seconds()?);

                    // Try refill remaining rows
                    let mut remaining_rows = VecDeque::new();
                    Self::try_refill_remaining_rows(&mut row_stream, &mut remaining_rows).await?;

                    // Get the rw_timestamp in the first row returned by the query if any.
                    // new_row_rw_timestamp == None means the query returns empty result.
                    let new_row_rw_timestamp: Option<i64> = remaining_rows.front().map(|row| {
                        std::str::from_utf8(row.index(0).as_ref().unwrap())
                            .unwrap()
                            .parse()
                            .unwrap()
                    });

                    // Check expected_timestamp against the rw_timestamp of the first row.
                    // Return an error if there is no new row or there is a mismatch.
                    if let Some(expected_timestamp) = expected_timestamp {
                        let expected_timestamp = *expected_timestamp;
                        if new_row_rw_timestamp.is_none()
                            || new_row_rw_timestamp.unwrap() != expected_timestamp
                        {
                            // Transition to Invalid state and return and error
                            self.state = State::Invalid;
                            return Err(ErrorCode::CatalogError(
                                format!(
                                    " No data found for rw_timestamp {:?}, data may have been recycled, please recreate cursor",
                                    convert_logstore_i64_to_unix_millis(expected_timestamp)
                                )
                                .into(),
                            )
                            .into());
                        }
                    }

                    // Return None if no data is found for the rw_timestamp in logstore.
                    // This happens when reaching EOF of logstore. This check cannot be moved before the
                    // expected_timestamp check to ensure that an error is returned on empty result when
                    // expected_timstamp is set.
                    if new_row_rw_timestamp.is_none() {
                        return Ok((None, pg_descs));
                    }

                    // Transition to the Fetch state
                    self.state = State::Fetch {
                        from_snapshot,
                        rw_timestamp: new_row_rw_timestamp.unwrap(),
                        row_stream,
                        pg_descs,
                        remaining_rows,
                    };
                }
                State::Fetch {
                    from_snapshot,
                    rw_timestamp,
                    row_stream,
                    pg_descs,
                    remaining_rows,
                } => {
                    let from_snapshot = *from_snapshot;
                    let rw_timestamp = *rw_timestamp;

                    // Try refill remaining rows
                    Self::try_refill_remaining_rows(row_stream, remaining_rows).await?;

                    if let Some(row) = remaining_rows.pop_front() {
                        // 1. Fetch the next row
                        let new_row = row.take();
                        if from_snapshot {
                            // 1a. The rw_timestamp in the table is all the same, so don't need to check.
                            return Ok((
                                Some(Row::new(Self::build_row_with_snapshot(new_row))),
                                pg_descs.clone(),
                            ));
                        }

                        let new_row_rw_timestamp: i64 = new_row
                            .get(0)
                            .unwrap()
                            .as_ref()
                            .map(|bytes| std::str::from_utf8(bytes).unwrap().parse().unwrap())
                            .unwrap();

                        if new_row_rw_timestamp != rw_timestamp {
                            // 1b. Find the next rw_timestamp.
                            // Initiate a new batch query to avoid query timeout and pinning version for too long.
                            // expected_timestamp shouold be set to ensure there is no data missing in the next query.
                            self.state = State::InitLogStoreQuery {
                                seek_timestamp: new_row_rw_timestamp,
                                expected_timestamp: Some(new_row_rw_timestamp),
                            };
                        } else {
                            // 1c. The rw_timestamp of this row is equal to self.rw_timestamp, return row
                            return Ok((
                                Some(Row::new(Self::build_row_with_logstore(
                                    new_row,
                                    rw_timestamp,
                                )?)),
                                pg_descs.clone(),
                            ));
                        }
                    } else {
                        // 2. Reach EOF for the current query.
                        // Initiate a new batch query using the rw_timestamp + 1.
                        // expected_timestamp don't need to be set as the next rw_timestamp is unknown.
                        self.state = State::InitLogStoreQuery {
                            seek_timestamp: rw_timestamp + 1,
                            expected_timestamp: None,
                        };
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

    async fn initiate_query(
        rw_timestamp: Option<i64>,
        subscription: &SubscriptionCatalog,
        handle_args: HandlerArgs,
    ) -> Result<(PgResponseStream, Vec<PgFieldDescriptor>)> {
        let query_stmt = if let Some(rw_timestamp) = rw_timestamp {
            Statement::Query(Box::new(gen_query_from_logstore_ge_rw_timestamp(
                &subscription.get_log_store_name(),
                rw_timestamp,
            )))
        } else {
            let subscription_from_table_name = ObjectName(vec![Ident::from(
                subscription.subscription_from_name.as_ref(),
            )]);
            Statement::Query(Box::new(gen_query_from_table_name(
                subscription_from_table_name,
            )))
        };
        let (row_stream, pg_descs) = create_stream_for_cursor(handle_args, query_stmt).await?;
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

    pub fn build_row_with_snapshot(row: Vec<Option<Bytes>>) -> Vec<Option<Bytes>> {
        let mut new_row = vec![None, Some(Bytes::from(1i16.to_string()))];
        new_row.extend(row);
        new_row
    }

    pub fn build_row_with_logstore(
        mut row: Vec<Option<Bytes>>,
        rw_timestamp: i64,
    ) -> Result<Vec<Option<Bytes>>> {
        let mut new_row = vec![Some(Bytes::from(
            convert_logstore_i64_to_unix_millis(rw_timestamp).to_string(),
        ))];
        // need remove kv_log_store_epoch
        new_row.extend(row.drain(1..row.len()).collect_vec());
        Ok(new_row)
    }

    pub fn build_desc(
        mut descs: Vec<PgFieldDescriptor>,
        from_snapshot: bool,
    ) -> Vec<PgFieldDescriptor> {
        let mut new_descs = vec![
            PgFieldDescriptor::new(
                "rw_timestamp".to_owned(),
                DataType::Int64.to_oid(),
                DataType::Int64.type_len(),
            ),
            PgFieldDescriptor::new(
                "op".to_owned(),
                DataType::Int16.to_oid(),
                DataType::Int16.type_len(),
            ),
        ];
        // need remove kv_log_store_epoch and kv_log_store_row_op
        if from_snapshot {
            new_descs.extend(descs)
        } else {
            assert_eq!(
                descs.get(0).unwrap().get_name(),
                KV_LOG_STORE_EPOCH,
                "Cursor query logstore: first column must be {}",
                KV_LOG_STORE_EPOCH
            );
            assert_eq!(
                descs.get(1).unwrap().get_name(),
                KV_LOG_STORE_ROW_OP,
                "Cursor query logstore: first column must be {}",
                KV_LOG_STORE_ROW_OP
            );
            new_descs.extend(descs.drain(2..descs.len()));
        }
        new_descs
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
        start_timestamp: Option<i64>,
        subscription: Arc<SubscriptionCatalog>,
        handle_args: &HandlerArgs,
    ) -> Result<()> {
        let cursor = SubscriptionCursor::new(
            cursor_name.clone(),
            start_timestamp,
            subscription,
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
