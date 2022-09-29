// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fmt::Formatter;

use futures::stream::BoxStream;
use futures::{stream, StreamExt};

use crate::pg_field_descriptor::PgFieldDescriptor;
use crate::pg_server::BoxedError;
use crate::types::Row;

pub type PgResultSet = BoxStream<'static, Result<Vec<Row>, BoxedError>>;
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
#[expect(non_camel_case_types, clippy::upper_case_acronyms)]
pub enum StatementType {
    INSERT,
    DELETE,
    UPDATE,
    SELECT,
    MOVE,
    FETCH,
    COPY,
    EXPLAIN,
    CREATE_TABLE,
    CREATE_MATERIALIZED_VIEW,
    CREATE_SOURCE,
    CREATE_SINK,
    CREATE_DATABASE,
    CREATE_SCHEMA,
    CREATE_USER,
    CREATE_INDEX,
    DESCRIBE_TABLE,
    GRANT_PRIVILEGE,
    DROP_TABLE,
    DROP_MATERIALIZED_VIEW,
    DROP_INDEX,
    DROP_SOURCE,
    DROP_SINK,
    DROP_SCHEMA,
    DROP_DATABASE,
    DROP_USER,
    REVOKE_PRIVILEGE,
    // Introduce ORDER_BY statement type cuz Calcite unvalidated AST has SqlKind.ORDER_BY. Note
    // that Statement Type is not designed to be one to one mapping with SqlKind.
    ORDER_BY,
    SET_OPTION,
    SHOW_PARAMETERS,
    SHOW_COMMAND,
    START_TRANSACTION,
    UPDATE_USER,
    ABORT,
    FLUSH,
    OTHER,
    // EMPTY is used when query statement is empty (e.g. ";").
    EMPTY,
    BEGIN,
    COMMIT,
    ROLLBACK,
}

impl std::fmt::Display for StatementType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

pub struct PgResponse {
    stmt_type: StatementType,
    // row count of effected row. Used for INSERT, UPDATE, DELETE, COPY, and other statements that
    // don't return rows.
    row_cnt: Option<i32>,
    notice: Option<String>,
    values_stream: PgResultSet,
    row_desc: Vec<PgFieldDescriptor>,
}

impl std::fmt::Debug for PgResponse {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PgResponse")
            .field("stmt_type", &self.stmt_type)
            .field("row_cnt", &self.row_cnt)
            .field("notice", &self.notice)
            .field("row_desc", &self.row_desc)
            .finish()
    }
}

impl StatementType {
    pub fn is_command(&self) -> bool {
        matches!(
            self,
            StatementType::INSERT
                | StatementType::DELETE
                | StatementType::UPDATE
                | StatementType::MOVE
                | StatementType::COPY
                | StatementType::FETCH
                | StatementType::SELECT
        )
    }

    pub fn is_dml(&self) -> bool {
        matches!(
            self,
            StatementType::INSERT | StatementType::DELETE | StatementType::UPDATE
        )
    }

    pub fn is_query(&self) -> bool {
        matches!(
            self,
            StatementType::SELECT
                | StatementType::EXPLAIN
                | StatementType::SHOW_COMMAND
                | StatementType::DESCRIBE_TABLE
        )
    }
}

impl PgResponse {
    pub fn new(
        stmt_type: StatementType,
        row_cnt: Option<i32>,
        values: Vec<Row>,
        row_desc: Vec<PgFieldDescriptor>,
    ) -> Self {
        Self {
            stmt_type,
            row_cnt,
            notice: None,
            values_stream: futures::stream::iter(vec![Ok(values)]).boxed(),
            row_desc,
        }
    }

    pub fn new_for_stream(
        stmt_type: StatementType,
        row_cnt: Option<i32>,
        values_stream: BoxStream<'static, Result<Vec<Row>, BoxedError>>,
        row_desc: Vec<PgFieldDescriptor>,
    ) -> Self {
        Self {
            stmt_type,
            row_cnt,
            values_stream,
            row_desc,
            notice: None,
        }
    }

    pub fn empty_result(stmt_type: StatementType) -> Self {
        let row_cnt = if stmt_type.is_query() { None } else { Some(0) };
        Self::new_for_stream(stmt_type, row_cnt, stream::empty().boxed(), vec![])
    }

    pub fn empty_result_with_notice(stmt_type: StatementType, notice: String) -> Self {
        let row_cnt = if stmt_type.is_query() { None } else { Some(0) };
        Self {
            stmt_type,
            row_cnt,
            values_stream: stream::empty().boxed(),
            row_desc: vec![],
            notice: Some(notice),
        }
    }

    pub fn get_stmt_type(&self) -> StatementType {
        self.stmt_type
    }

    pub fn get_notice(&self) -> Option<String> {
        self.notice.clone()
    }

    pub fn get_effected_rows_cnt(&self) -> Option<i32> {
        self.row_cnt
    }

    pub fn is_query(&self) -> bool {
        self.stmt_type.is_query()
    }

    pub fn is_empty(&self) -> bool {
        self.stmt_type == StatementType::EMPTY
    }

    pub fn get_row_desc(&self) -> Vec<PgFieldDescriptor> {
        self.row_desc.clone()
    }

    pub fn values_stream(&mut self) -> &mut PgResultSet {
        &mut self.values_stream
    }
}
