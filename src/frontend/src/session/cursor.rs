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
use pgwire::types::{Format, Row};
use risingwave_common::session_config::QueryMode;
use risingwave_sqlparser::ast::ObjectName;

use super::{transaction, PgResponseStream};
use crate::error::{ErrorCode, Result, RwError};
use crate::handler::query::{distribute_execute, local_execute, BatchPlanFragmenterResult};
use crate::handler::util::{to_pg_field, DataChunkToRowSetAdapter};
use crate::session::SessionImpl;

pub struct Cursor {
    plan_fragmenter_result: BatchPlanFragmenterResult,
    formats: Vec<Format>,
    session: Arc<SessionImpl>,

    row_stream: Option<PgResponseStream>,
    pg_descs: Option<Vec<PgFieldDescriptor>>,
    remaining_rows: VecDeque<Row>,
}

impl Cursor {
    pub fn new(
        plan_fragmenter_result: BatchPlanFragmenterResult,
        formats: Vec<Format>,
        session: Arc<SessionImpl>,
    ) -> Self {
        Self {
            plan_fragmenter_result,
            formats,
            session,
            row_stream: None,
            pg_descs: None,
            remaining_rows: VecDeque::<Row>::new(),
        }
    }

    async fn create_stream(&mut self) -> Result<()> {
        let BatchPlanFragmenterResult {
            plan_fragmenter,
            query_mode,
            schema,
            stmt_type,
            ..
        } = self.plan_fragmenter_result.clone();
        assert_eq!(stmt_type, StatementType::SELECT);

        let can_timeout_cancel = true;

        // let query_start_time = Instant::now();
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
                local_execute(self.session.clone(), query, can_timeout_cancel).await?,
                column_types,
                self.formats.clone(),
                self.session.clone(),
            )),
            // Local mode do not support cancel tasks.
            QueryMode::Distributed => {
                PgResponseStream::DistributedQuery(DataChunkToRowSetAdapter::new(
                    distribute_execute(self.session.clone(), query, can_timeout_cancel).await?,
                    column_types,
                    self.formats.clone(),
                    self.session.clone(),
                ))
            }
        };

        self.row_stream = Some(row_stream);
        self.pg_descs = Some(pg_descs);
        Ok(())
    }

    pub async fn next_once(&mut self) -> Result<Option<Row>> {
        if self.row_stream.is_none() {
            self.create_stream().await?;
        }
        while self.remaining_rows.is_empty() {
            let rows = self.row_stream.as_mut().unwrap().next().await;
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

    pub fn pg_descs(&self) -> Result<Vec<PgFieldDescriptor>> {
        Ok(self.pg_descs.clone().unwrap())
    }
}

impl SessionImpl {
    pub fn is_in_transaction(&self) -> bool {
        match &*self.txn.lock() {
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
            return Err(ErrorCode::InternalError(format!(
                "cursor \"{}\" already exists",
                cursor_name,
            ))
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
            None => Err(ErrorCode::InternalError(format!(
                "cursor \"{}\" does not exist",
                cursor_name
            ))
            .into()),
        }
    }

    pub async fn curosr_next(
        &self,
        cursor_name: &ObjectName,
        count: Option<i32>,
    ) -> Result<Vec<Row>> {
        if let Some(cursor) = self.cursors.lock().await.get_mut(cursor_name) {
            cursor.next(count).await
        } else {
            Err(
                ErrorCode::InternalError(format!("cursor \"{}\" does not exist", cursor_name,))
                    .into(),
            )
        }
    }

    pub async fn pg_descs(&self, cursor_name: &ObjectName) -> Result<Vec<PgFieldDescriptor>> {
        if let Some(cursor) = self.cursors.lock().await.get(cursor_name) {
            cursor.pg_descs()
        } else {
            Err(
                ErrorCode::InternalError(format!("cursor \"{}\" does not exist", cursor_name,))
                    .into(),
            )
        }
    }
}
