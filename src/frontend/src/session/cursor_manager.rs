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
use core::ops::Index;
use core::time::Duration;
use std::collections::{HashMap, VecDeque};
use std::time::Instant;

use bytes::Bytes;
use futures::StreamExt;
use itertools::Itertools;
use pgwire::pg_field_descriptor::PgFieldDescriptor;
use pgwire::types::Row;
use risingwave_common::types::DataType;
use risingwave_sqlparser::ast::{Ident, ObjectName, Statement};

use crate::error::{ErrorCode, Result, RwError};
use crate::handler::declare_cursor::create_stream_for_cursor;
use crate::handler::util::{
    convert_logstore_i64_to_unix_millis, gen_query_from_logstore_ge_rw_timestamp,
    gen_query_from_table_name,
};
use crate::handler::HandlerArgs;
use crate::{Binder, PgResponseStream};

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
                Some(row) => {
                    row.map_err(|err| RwError::from(ErrorCode::InternalError(format!("{}", err))))?
                }
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

pub struct SubscriptionCursor {
    cursor_name: String,
    rw_timestamp: i64,
    is_snapshot: bool,
    subscription_name: ObjectName,
    cursor_need_drop_time: Instant,
    state: State,
}

impl SubscriptionCursor {
    pub fn new(
        cursor_name: String,
        start_timestamp: i64,
        is_snapshot: bool,
        subscription_name: ObjectName,
        retention_times: Duration,
    ) -> Result<Self> {
        Ok(Self {
            cursor_name,
            rw_timestamp: start_timestamp,
            is_snapshot,
            subscription_name,
            cursor_need_drop_time: Instant::now() + retention_times,
            state: State::InitiateQuery(None),
        })
    }

    async fn transition_to_fetch(
        &mut self,
        row_stream: PgResponseStream,
        pg_descs: Vec<PgFieldDescriptor>,
        data_chunk_cache: VecDeque<Row>,
    ) -> Result<()> {
        // // Cursor created based on table, no need to update start_timestamp
        // if !self.is_snapshot {
        //     let data_chunk_cache = self
        //         .row_stream
        //         .next()
        //         .await
        //         .unwrap_or_else(|| Ok(Vec::new()))
        //         .map_err(|e| {
        //             ErrorCode::InternalError(format!(
        //                 "Cursor get next chunk error {:?}",
        //                 e.to_string()
        //             ))
        //         })?;
        //     // Use the first line of the log store to update start_timestamp
        //     let query_timestamp = data_chunk_cache
        //         .get(0)
        //         .map(|row| {
        //             row.index(0)
        //                 .as_ref()
        //                 .map(|bytes| std::str::from_utf8(bytes).unwrap().parse().unwrap())
        //                 .unwrap()
        //         })
        //         .unwrap_or_else(|| self.rw_timestamp);
        //     if need_check_timestamp
        //         && (data_chunk_cache.is_empty() || query_timestamp != self.rw_timestamp)
        //     {
        //         // If the previous cursor returns next_rw_timestamp, then this check is triggered,
        //         // and query_timestamp and start_timestamp must be equal to each other to prevent data errors caused by two long cursor times
        //         return Err(ErrorCode::CatalogError(format!(
        //                 " No data found for rw_timestamp {:?}, data may have been recycled, please recreate cursor"
        //             ,convert_logstore_i64_to_unix_millis(self.rw_timestamp)).into()).into());
        //     }
        //     self.data_chunk_cache = VecDeque::from(data_chunk_cache);
        //     self.rw_timestamp = query_timestamp;
        // };
        // let pg_descs = mem::take(&mut self.pg_descs);
        // self.pg_descs = build_desc(pg_descs, self.is_snapshot);
        self.state = State::Fetch {row_stream, pg_descs, data_chunk_cache};
        Ok(())
    }

    fn transition_to_initiate_query(
        &mut self,
        next_timestamp: i64,
        expected_timestamp: Option<i64>,
    ) {
        assert!(next_timestamp >= self.rw_timestamp);
        self.is_snapshot = false;
        self.rw_timestamp = next_timestamp;
        self.state = State::InitiateQuery(expected_timestamp);
    }

    async fn initiate_query(
        &mut self,
        handle_args: HandlerArgs,
    ) -> Result<(PgResponseStream, Vec<PgFieldDescriptor>)> {
        let session = handle_args.session.clone();
        let db_name = session.database();
        let (schema_name, subscription_name) =
            Binder::resolve_schema_qualified_name(db_name, self.subscription_name.clone())?;
        let subscription = session.get_subscription_by_name(schema_name, &subscription_name)?;

        let query_stmt = if self.is_snapshot {
            let subscription_from_table_name = ObjectName(vec![Ident::from(
                subscription.subscription_from_name.as_ref(),
            )]);
            Statement::Query(Box::new(gen_query_from_table_name(
                subscription_from_table_name,
            )));
        } else {
            Statement::Query(Box::new(gen_query_from_logstore_ge_rw_timestamp(
                &subscription.get_log_store_name()?,
                self.rw_timestamp,
            )))
        };
        let (row_stream, pg_descs) = create_stream_for_cursor(handle_args, query_stmt).await?;
        self.cursor_need_drop_time =
            Instant::now() + Duration::from_secs(subscription.get_retention_seconds()?);
        if let Some(row_set) = self.row_stream.next().await {
            // 2a. Get the data from the stream and consume it in the next cycle
            self.data_chunk_cache = VecDeque::from(row_set.map_err(|e| {
                ErrorCode::InternalError(format!("Cursor get next chunk error {:?}", e.to_string()))
            })?);
        }
        Ok((row_stream, pg_descs))
    }

    async fn fill_cache(&mut self) -> Result<()> {
        
        Ok(())
    }

    async fn next_row(&mut self) -> Result<Option<Row>> {
        self.data_chunk_cache.pop_front().or_else(|| async {
            self.fill_cache().await?;
            self.data_chunk_cache.pop_front()
        })
    }

    pub async fn next_once(&mut self, handle_args: HandlerArgs) -> Result<Option<Row>> {
        loop {
            match self.state {
                State::InitiateQuery(expected_timestamp) => {
                    // 3. Query data with next_rw_timestamp
                    let (row_stream, pg_desc) = self.initiate_query(handle_args.clone()).await?;
                    if let Some(row_set) = self.row_stream.next().await {
                        let data_chunk_cache = VecDeque::from(row_set.map_err(|e| {
                            ErrorCode::InternalError(format!("Cursor get next chunk error {:?}", e.to_string()))
                        })?);
                    }
                }
                State::Fetch(row_stream, pg_desc, data_chunk_cache) => {
                    // Return result, and checkout state
                    if let Some(row) = self.next_row().await? {
                        // 1. fetch data
                        let new_row = row.take();
                        if self.is_snapshot {
                            // 1a. The rw_timestamp in the table is all the same, so don't need to check.
                            return Ok(Some(Row::new(build_row_with_snapshot(new_row))));
                        }

                        let timestamp_row: i64 = new_row
                            .get(0)
                            .unwrap()
                            .as_ref()
                            .map(|bytes| std::str::from_utf8(bytes).unwrap().parse().unwrap())
                            .unwrap();

                        if timestamp_row != self.rw_timestamp {
                            // 1b. Find next_rw_timestamp, need update cursor with next_rw_timestamp.
                            self.transition_to_initiate_query(timestamp_row, Some(timestamp_row));
                        } else {
                            // 1c. The rw_timestamp of this row is equal to self.rw_timestamp, return row
                            return Ok(Some(Row::new(build_row_with_logstore(
                                new_row,
                                timestamp_row,
                            )?)));
                        }
                    } else {
                        // 2. No data was fetched and next_rw_timestamp was not found, so need to query using the rw_timestamp+1. So we don't need to update self.rw_timestamp
                        self.transition_to_initiate_query(self.rw_timestamp + 1, None);
                    }
                }
            }
        }
    }

    pub async fn next(&mut self, count: u32, handle_args: HandlerArgs) -> Result<Vec<Row>> {
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
        } else if let Some(row) = self.next_once(handle_args).await? {
            Ok(vec![row])
        } else {
            Ok(vec![])
        }
    }

    pub fn pg_descs(&self) -> Vec<PgFieldDescriptor> {
        self.pg_descs.clone()
    }
}

enum State {
    InitiateQuery(Option<i64>),
    Fetch {
        row_stream: PgResponseStream,
        pg_descs: Vec<PgFieldDescriptor>,
        data_chunk_cache: VecDeque<Row>,
    },
    Invalid,
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

pub fn build_desc(mut descs: Vec<PgFieldDescriptor>, is_snapshot: bool) -> Vec<PgFieldDescriptor> {
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
    if is_snapshot {
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

#[derive(Default)]
pub struct CursorManager {
    cursor_map: tokio::sync::Mutex<HashMap<String, Cursor>>,
}

impl CursorManager {
    pub async fn add_subscription_cursor(
        &self,
        cursor_name: String,
        start_timestamp: i64,
        is_snapshot: bool,
        subscription_name: ObjectName,
        retention_secs: u64,
    ) -> Result<()> {
        let cursor = SubscriptionCursor::new(
            cursor_name.clone(),
            start_timestamp,
            is_snapshot,
            subscription_name.clone(),
            Duration::from_secs(retention_secs),
        )?;
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
