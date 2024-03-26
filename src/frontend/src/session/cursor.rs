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

use std::collections::VecDeque;
use std::sync::Arc;

use futures::StreamExt;
use itertools::Itertools;
use pgwire::pg_field_descriptor::PgFieldDescriptor;
use pgwire::pg_response::StatementType;
use pgwire::types::Row;
use risingwave_common::session_config::QueryMode;
use risingwave_sqlparser::ast::ObjectName;

use super::{transaction, PgResponseStream};
use crate::error::{ErrorCode, Result, RwError};
use crate::handler::query::{distribute_execute, local_execute, BatchPlanFragmenterResult};
use crate::handler::util::{to_pg_field, DataChunkToRowSetAdapter};
use crate::session::SessionImpl;

pub struct Cursor {
    row_stream: PgResponseStream,
    pg_descs: Vec<PgFieldDescriptor>,
    remaining_rows: VecDeque<Row>,
}

impl Cursor {
    pub async fn new(
        plan_fragmenter_result: BatchPlanFragmenterResult,
        session: Arc<SessionImpl>,
    ) -> Result<Self> {
        let (row_stream, pg_descs) = Self::create_stream(plan_fragmenter_result, session).await?;
        Ok(Self {
            row_stream,
            pg_descs,
            remaining_rows: VecDeque::<Row>::new(),
        })
    }

    async fn create_stream(
        plan_fragmenter_result: BatchPlanFragmenterResult,
        session: Arc<SessionImpl>,
    ) -> Result<(PgResponseStream, Vec<PgFieldDescriptor>)> {
        let BatchPlanFragmenterResult {
            plan_fragmenter,
            query_mode,
            schema,
            stmt_type,
            ..
        } = plan_fragmenter_result;
        assert_eq!(stmt_type, StatementType::SELECT);

        let can_timeout_cancel = true;

        let query = plan_fragmenter.generate_complete_query().await?;
        tracing::trace!("Generated query after plan fragmenter: {:?}", &query);

        let pg_descs = schema
            .fields()
            .iter()
            .map(to_pg_field)
            .collect::<Vec<PgFieldDescriptor>>();
        let column_types = schema.fields().iter().map(|f| f.data_type()).collect_vec();

        let row_stream: PgResponseStream = match query_mode {
            QueryMode::Auto => unreachable!(),
            QueryMode::Local => PgResponseStream::LocalQuery(DataChunkToRowSetAdapter::new(
                local_execute(session.clone(), query, can_timeout_cancel).await?,
                column_types,
                vec![],
                session.clone(),
            )),
            // Local mode do not support cancel tasks.
            QueryMode::Distributed => {
                PgResponseStream::DistributedQuery(DataChunkToRowSetAdapter::new(
                    distribute_execute(session.clone(), query, can_timeout_cancel).await?,
                    column_types,
                    vec![],
                    session.clone(),
                ))
            }
        };

        Ok((row_stream, pg_descs))
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

    pub async fn next(&mut self, count: Option<i32>) -> Result<Vec<Row>> {
        // `FETCH NEXT` is equivalent to `FETCH 1`.
        let fetch_count = count.unwrap_or(1);
        let mut ans = vec![];
        if fetch_count <= 0 {
            Err(crate::error::ErrorCode::InternalError(
                "FETCH a non-positive count is not supported yet".to_string(),
            )
            .into())
        } else {
            let mut cur = 0;
            while cur < fetch_count
                && let Some(row) = self.next_once().await?
            {
                cur += 1;
                ans.push(row);
            }
            Ok(ans)
        }
    }

    pub fn pg_descs(&self) -> Vec<PgFieldDescriptor> {
        self.pg_descs.clone()
    }
}

impl SessionImpl {
    pub fn is_in_transaction(&self) -> bool {
        *self.in_rw_txn.lock()
            || match &*self.txn.lock() {
                transaction::State::Initial | transaction::State::Implicit(_) => false,
                transaction::State::Explicit(_) => true,
            }
    }

    pub async fn add_cursor(&self, cursor_name: ObjectName, cursor: Cursor) -> Result<()> {
        if self
            .cursors
            .lock()
            .await
            .try_insert(cursor_name.clone(), cursor)
            .is_err()
        {
            return Err(ErrorCode::CatalogError(
                format!("cursor \"{cursor_name}\" already exists").into(),
            )
            .into());
        }
        Ok(())
    }

    pub async fn drop_all_cursors(&self) {
        self.cursors.lock().await.clear();
    }

    pub async fn drop_cursor(&self, cursor_name: ObjectName) -> Result<()> {
        match self.cursors.lock().await.remove(&cursor_name) {
            Some(_) => Ok(()),
            None => Err(ErrorCode::CatalogError(
                format!("cursor \"{cursor_name}\" does not exist").into(),
            )
            .into()),
        }
    }

    pub async fn cursor_next(
        &self,
        cursor_name: &ObjectName,
        count: Option<i32>,
    ) -> Result<(Vec<Row>, Vec<PgFieldDescriptor>)> {
        if let Some(cursor) = self.cursors.lock().await.get_mut(cursor_name) {
            Ok((cursor.next(count).await?, cursor.pg_descs()))
        } else {
            Err(
                ErrorCode::CatalogError(format!("cursor \"{cursor_name}\" does not exist").into())
                    .into(),
            )
        }
    }
}
