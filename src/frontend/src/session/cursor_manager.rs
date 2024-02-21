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
use std::collections::{HashMap, VecDeque};

use bytes::Bytes;
use futures::StreamExt;
use itertools::Itertools;
use pgwire::pg_field_descriptor::PgFieldDescriptor;
use pgwire::types::Row;
use risingwave_common::types::DataType;
use risingwave_sqlparser::ast::ObjectName;

use crate::error::{ErrorCode, Result};
use crate::handler::util::convert_logstore_i64_to_epoch;
use crate::handler::RwPgResponse;
pub struct Cursor {
    cursor_name: String,
    rw_pg_response: RwPgResponse,
    data_chunk_cache: VecDeque<Row>,
    rw_timestamp: i64,
    is_snapshot: bool,
    subscription_name: ObjectName,
    pg_desc: Vec<PgFieldDescriptor>,
}

impl Cursor {
    pub async fn new(
        cursor_name: String,
        mut rw_pg_response: RwPgResponse,
        start_timestamp: i64,
        is_snapshot: bool,
        need_check_timestamp: bool,
        subscription_name: ObjectName,
    ) -> Result<Self> {
        let (rw_timestamp, data_chunk_cache) = if is_snapshot {
            (start_timestamp, vec![])
        } else {
            let data_chunk_cache = rw_pg_response
                .values_stream()
                .next()
                .await
                .unwrap_or_else(|| Ok(Vec::new()))
                .map_err(|e| {
                    ErrorCode::InternalError(format!(
                        "Cursor get next chunk error {:?}",
                        e.to_string()
                    ))
                })?;
            let query_timestamp = data_chunk_cache
                .get(0)
                .map(|row| {
                    println!("123");
                    row.index(0)
                        .as_ref()
                        .map(|bytes| std::str::from_utf8(bytes).unwrap().parse().unwrap())
                        .unwrap()
                })
                .unwrap_or_else(|| start_timestamp);
            if need_check_timestamp
                && (data_chunk_cache.is_empty() || query_timestamp != start_timestamp)
            {
                return Err(ErrorCode::InternalError(format!(
                    " No data found for rw_timestamp {:?}, data may have been recycled, please recreate cursor"
                ,convert_logstore_i64_to_epoch(start_timestamp))).into());
            }
            (query_timestamp, data_chunk_cache)
        };
        let pg_desc = build_desc(rw_pg_response.row_desc(), is_snapshot);
        // check timestamp.
        Ok(Self {
            cursor_name,
            rw_pg_response,
            data_chunk_cache: VecDeque::from(data_chunk_cache),
            rw_timestamp,
            is_snapshot,
            subscription_name,
            pg_desc,
        })
    }

    pub async fn next(&mut self) -> Result<CursorRowValue> {
        let stream = self.rw_pg_response.values_stream();
        loop {
            if self.data_chunk_cache.is_empty() {
                if let Some(row_set) = stream.next().await {
                    self.data_chunk_cache = VecDeque::from(row_set.map_err(|e| {
                        ErrorCode::InternalError(format!(
                            "Cursor get next chunk error {:?}",
                            e.to_string()
                        ))
                    })?);
                } else {
                    return Ok(CursorRowValue::QueryWithStartRwTimestamp(
                        self.rw_timestamp,
                        self.subscription_name.clone(),
                    ));
                }
            }
            println!("desc:{:?}", self.pg_desc);
            println!("data_chunk_cache:{:?}", self.data_chunk_cache);
            if let Some(row) = self.data_chunk_cache.pop_front() {
                let new_row = row.take();
                if self.is_snapshot {
                    return Ok(CursorRowValue::Row((
                        Row::new(build_row_with_snapshot(new_row, self.rw_timestamp)),
                        self.pg_desc.clone(),
                    )));
                }

                let timestamp_row: i64 = new_row
                    .get(0)
                    .unwrap()
                    .as_ref()
                    .map(|bytes| std::str::from_utf8(bytes).unwrap().parse().unwrap())
                    .unwrap();

                println!(
                    "timestamp_row:{:?},self.rw_timestamp{:?}",
                    timestamp_row, self.rw_timestamp
                );
                if timestamp_row != self.rw_timestamp {
                    return Ok(CursorRowValue::QueryWithNextRwTimestamp(
                        timestamp_row,
                        self.subscription_name.clone(),
                    ));
                } else {
                    return Ok(CursorRowValue::Row((
                        Row::new(build_row_with_logstore(new_row, timestamp_row)?),
                        self.pg_desc.clone(),
                    )));
                }
            }
        }
    }
}

pub fn build_row_with_snapshot(row: Vec<Option<Bytes>>, rw_timestamp: i64) -> Vec<Option<Bytes>> {
    let mut new_row = vec![
        Some(Bytes::from(
            convert_logstore_i64_to_epoch(rw_timestamp).to_string(),
        )),
        Some(Bytes::from(1i16.to_string())),
    ];
    new_row.extend(row);
    new_row
}

pub fn build_row_with_logstore(
    mut row: Vec<Option<Bytes>>,
    rw_timestamp: i64,
) -> Result<Vec<Option<Bytes>>> {
    // remove sqr_id, vnode ,_row_id
    let mut new_row = vec![Some(Bytes::from(
        convert_logstore_i64_to_epoch(rw_timestamp).to_string(),
    ))];
    new_row.extend(row.drain(3..row.len() - 1).collect_vec());
    Ok(new_row)
}

pub fn build_desc(mut descs: Vec<PgFieldDescriptor>, is_snapshot: bool) -> Vec<PgFieldDescriptor> {
    let mut new_descs = vec![
        PgFieldDescriptor::new(
            "rw_timestamp".to_owned(),
            DataType::Varchar.to_oid(),
            DataType::Varchar.type_len(),
        ),
        PgFieldDescriptor::new(
            "op".to_owned(),
            DataType::Int16.to_oid(),
            DataType::Int16.type_len(),
        ),
    ];
    if is_snapshot {
        new_descs.extend(descs)
    } else {
        new_descs.extend(descs.drain(4..descs.len() - 1));
    }
    new_descs
}

pub enum CursorRowValue {
    Row((Row, Vec<PgFieldDescriptor>)),
    QueryWithNextRwTimestamp(i64, ObjectName),
    QueryWithStartRwTimestamp(i64, ObjectName),
}
#[derive(Default)]
pub struct CursorManager {
    cursor_map: HashMap<String, Cursor>,
}

impl CursorManager {
    pub fn add_cursor(&mut self, cursor: Cursor) -> Result<()> {
        self.cursor_map.insert(cursor.cursor_name.clone(), cursor);
        Ok(())
    }

    pub fn update_cursor(&mut self, cursor: Cursor) -> Result<()> {
        self.cursor_map.insert(cursor.cursor_name.clone(), cursor);
        Ok(())
    }

    pub fn remove_cursor(&mut self, cursor_name: String) -> Result<()> {
        self.cursor_map.remove(&cursor_name);
        Ok(())
    }

    pub async fn get_row_with_cursor(&mut self, cursor_name: String) -> Result<CursorRowValue> {
        if let Some(cursor) = self.cursor_map.get_mut(&cursor_name) {
            cursor.next().await
        } else {
            Err(ErrorCode::ItemNotFound(format!("Don't find cursor `{}`", cursor_name)).into())
        }
    }
}
