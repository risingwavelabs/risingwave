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
use std::time::Instant;

use futures::StreamExt;
use itertools::Itertools;
use pgwire::pg_field_descriptor::PgFieldDescriptor;
use pgwire::pg_response::{PgResponse, StatementType};
use pgwire::types::{Format, Row};
use postgres_types::FromSql;
use risingwave_common::catalog::Schema;
use risingwave_common::session_config::QueryMode;
use risingwave_common::types::{DataType, Datum};
use risingwave_sqlparser::ast::{ObjectName, SetExpr, Statement};

use super::{transaction, PgResponseStream, RwPgResponse};
use crate::binder::{Binder, BoundStatement};
use crate::catalog::TableId;
use crate::error::{ErrorCode, Result, RwError};
use crate::handler::privilege::resolve_privileges;
use crate::handler::query::{distribute_execute, local_execute, BatchPlanFragmenterResult};
use crate::handler::util::{to_pg_field, DataChunkToRowSetAdapter};
use crate::handler::HandlerArgs;
use crate::optimizer::plan_node::Explain;
use crate::optimizer::{
    ExecutionModeDecider, OptimizerContext, OptimizerContextRef, RelationCollectorVisitor,
    SysTableVisitor,
};
use crate::planner::Planner;
use crate::scheduler::plan_fragmenter::Query;
use crate::scheduler::worker_node_manager::WorkerNodeSelector;
use crate::scheduler::{
    BatchPlanFragmenter, DistributedQueryStream, ExecutionContext, ExecutionContextRef,
    LocalQueryExecution, LocalQueryStream,
};
use crate::session::SessionImpl;
use crate::PlanRef;

pub struct Cursor {
    plan_fragmenter_result: BatchPlanFragmenterResult,
    formats: Vec<Format>,
    row_stream: Option<PgResponseStream>,
    pg_descs: Option<Vec<PgFieldDescriptor>>,
    remaining_rows: VecDeque<Row>,
}

impl Cursor {
    pub fn new(plan_fragmenter_result: BatchPlanFragmenterResult, formats: Vec<Format>) -> Self {
        Self {
            plan_fragmenter_result,
            formats,
            row_stream: None,
            pg_descs: None,
            remaining_rows: VecDeque::<Row>::new(),
        }
    }

    async fn create_stream(&mut self, session: Arc<SessionImpl>) -> Result<()> {
        let BatchPlanFragmenterResult {
            plan_fragmenter,
            query_mode,
            schema,
            stmt_type,
            ..
        } = self.plan_fragmenter_result.clone();
        assert_eq!(stmt_type, StatementType::SELECT);

        let can_timeout_cancel = true;

        let query_start_time = Instant::now();
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
                self.formats.clone(),
                session.clone(),
            )),
            // Local mode do not support cancel tasks.
            QueryMode::Distributed => {
                PgResponseStream::DistributedQuery(DataChunkToRowSetAdapter::new(
                    distribute_execute(session.clone(), query, can_timeout_cancel).await?,
                    column_types,
                    self.formats.clone(),
                    session.clone(),
                ))
            }
        };

        self.row_stream = Some(row_stream);
        self.pg_descs = Some(pg_descs);
        Ok(())
    }

    pub async fn next(&mut self, session: Arc<SessionImpl>) -> Result<Option<Row>> {
        if self.row_stream.is_none() {
            self.create_stream(session);
        }
        if self.remaining_rows.is_empty() {
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
}

impl SessionImpl {
    pub fn is_in_transaction(&self) -> bool {
        match &*self.txn.lock() {
            transaction::State::Initial | transaction::State::Implicit(_) => false,
            transaction::State::Explicit(_) => true,
        }
    }

    pub fn add_cursor(&self, cursor_name: ObjectName, cursor: Cursor) -> Result<()> {
        if self.cursors.contains_key(&cursor_name) {
            return Err(ErrorCode::InternalError(format!(
                "cursor \"{}\" already exists",
                cursor_name,
            ))
            .into());
        }
        self.cursors.insert(cursor_name, cursor);
        Ok(())
    }

    pub async fn next(
        &mut self,
        cursor_name: &ObjectName,
        session: Arc<SessionImpl>,
    ) -> Result<Option<Row>> {
        if let Some(cursor) = self.cursors.get_mut(cursor_name) {
            cursor.next(session).await
        } else {
            Err(
                ErrorCode::InternalError(format!("cursor \"{}\" does not exist", cursor_name,))
                    .into(),
            )
        }
    }
}
