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

use crate::pg_field_descriptor::PgFieldDescriptor;
use crate::types::Row;

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
#[expect(non_camel_case_types)]
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
}

impl std::fmt::Display for StatementType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[derive(Debug)]
pub struct PgResponse {
    stmt_type: StatementType,
    row_cnt: i32,
    notice: Option<String>,
    values: Vec<Row>,
    // Used for row_limit mode to indicate whether run out of data
    row_end: bool,
    row_desc: Vec<PgFieldDescriptor>,
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
}

impl PgResponse {
    pub fn new(
        stmt_type: StatementType,
        row_cnt: i32,
        values: Vec<Row>,
        row_desc: Vec<PgFieldDescriptor>,
        row_end: bool,
    ) -> Self {
        Self {
            stmt_type,
            row_cnt,
            values,
            row_desc,
            row_end,
            notice: None,
        }
    }

    pub fn empty_result(stmt_type: StatementType) -> Self {
        Self::new(stmt_type, 0, vec![], vec![], true)
    }

    pub fn empty_result_with_notice(stmt_type: StatementType, notice: String) -> Self {
        Self {
            stmt_type,
            row_cnt: 0,
            values: vec![],
            row_desc: vec![],
            row_end: true,
            notice: Some(notice),
        }
    }

    pub fn get_stmt_type(&self) -> StatementType {
        self.stmt_type
    }

    pub fn get_notice(&self) -> Option<String> {
        self.notice.clone()
    }

    pub fn get_effected_rows_cnt(&self) -> i32 {
        self.row_cnt
    }

    pub fn is_query(&self) -> bool {
        matches!(
            self.stmt_type,
            StatementType::SELECT
                | StatementType::EXPLAIN
                | StatementType::SHOW_COMMAND
                | StatementType::DESCRIBE_TABLE
        )
    }

    pub fn is_empty(&self) -> bool {
        self.stmt_type == StatementType::EMPTY
    }

    pub fn is_row_end(&self) -> bool {
        self.row_end
    }

    pub fn get_row_desc(&self) -> Vec<PgFieldDescriptor> {
        self.row_desc.clone()
    }

    pub fn iter(&self) -> impl Iterator<Item = &Row> + '_ {
        self.values.iter()
    }

    pub fn values(&self) -> Vec<Row> {
        self.values.clone()
    }
}
