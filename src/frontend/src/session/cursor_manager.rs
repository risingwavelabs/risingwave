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
use std::collections::HashMap;

use futures::StreamExt;
use pgwire::pg_field_descriptor::PgFieldDescriptor;
use pgwire::types::Row;
use risingwave_common::error::{ErrorCode, Result};
use risingwave_sqlparser::ast::ObjectName;

use crate::handler::RwPgResponse;
pub struct Cursor {
    cursor_name: String,
    rw_pg_response: RwPgResponse,
    data_chunk_cache: Vec<Row>,
    rw_timestamp: u64,
    is_snapshot: bool,
    subscription_name: ObjectName,
    pg_desc: Vec<PgFieldDescriptor>,
}

impl Cursor {
    pub async fn new(
        cursor_name: String,
        mut rw_pg_response: RwPgResponse,
        start_timestamp: u64,
        is_snapshot: bool,
        subscription_name: ObjectName,
    ) -> Result<Self> {
        let rw_timestamp = if is_snapshot {
            start_timestamp
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
            data_chunk_cache
                .get(0)
                .map(|row| {
                    row.index(0)
                        .as_ref()
                        .map(|bytes| u64::from_be_bytes(bytes.as_ref().try_into().unwrap()))
                        .unwrap()
                })
                .unwrap_or_else(|| start_timestamp)
        };
        let pg_desc = rw_pg_response.row_desc();
        // check timestamp.
        Ok(Self {
            cursor_name,
            rw_pg_response,
            data_chunk_cache: Vec::new(),
            rw_timestamp,
            is_snapshot,
            subscription_name,
            pg_desc,
        })
    }

    pub async fn next(&mut self) -> Result<CursorRowValue> {
        if self.data_chunk_cache.is_empty() {
            if let Some(row_set) = self.rw_pg_response.values_stream().next().await {
                self.data_chunk_cache = row_set.map_err(|e| {
                    ErrorCode::InternalError(format!(
                        "Cursor get next chunk error {:?}",
                        e.to_string()
                    ))
                })?;
            } else {
                return Ok(CursorRowValue::NextQuery(
                    self.rw_timestamp + 1,
                    self.subscription_name.clone(),
                ));
            }
        }
        match self.data_chunk_cache.pop() {
            Some(row) => {
                let new_row = row.take();
                if self.is_snapshot {
                    Ok(CursorRowValue::Row((
                        Row::new(new_row),
                        self.pg_desc.clone(),
                    )))
                } else if true {
                    Ok(CursorRowValue::Row((
                        Row::new(new_row),
                        self.pg_desc.clone(),
                    )))
                } else {
                    Ok(CursorRowValue::NextQuery(
                        self.rw_timestamp + 1,
                        self.subscription_name.clone(),
                    ))
                }
            }

            None => Ok(CursorRowValue::NextQuery(
                self.rw_timestamp + 1,
                self.subscription_name.clone(),
            )),
        }
    }
}

pub enum CursorRowValue {
    Row((Row, Vec<PgFieldDescriptor>)),
    NextQuery(u64, ObjectName),
}
pub struct CursorManager {
    cursor_map: HashMap<String, Cursor>,
}
impl CursorManager {
    pub fn new() -> Self {
        Self {
            cursor_map: HashMap::new(),
        }
    }

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
            return Err(
                ErrorCode::ItemNotFound(format!("Don't find cursor `{}`", cursor_name)).into(),
            );
        }
    }
}
