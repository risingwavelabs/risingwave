// Copyright 2023 RisingWave Labs
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

use std::fmt::Formatter;
use std::pin::Pin;

use futures::{Future, FutureExt, Stream, StreamExt};

use crate::error::PsqlError;
use crate::pg_field_descriptor::PgFieldDescriptor;
use crate::pg_server::BoxedError;
use crate::types::Row;

pub type RowSet = Vec<Row>;
pub type RowSetResult = Result<RowSet, BoxedError>;
pub trait ValuesStream = Stream<Item = RowSetResult> + Unpin + Send;

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
#[expect(non_camel_case_types, clippy::upper_case_acronyms)]
pub enum StatementType {
    INSERT,
    INSERT_RETURNING,
    DELETE,
    DELETE_RETURNING,
    UPDATE,
    UPDATE_RETURNING,
    SELECT,
    MOVE,
    FETCH,
    COPY,
    EXPLAIN,
    CREATE_TABLE,
    CREATE_MATERIALIZED_VIEW,
    CREATE_VIEW,
    CREATE_SOURCE,
    CREATE_SINK,
    CREATE_DATABASE,
    CREATE_SCHEMA,
    CREATE_USER,
    CREATE_INDEX,
    CREATE_FUNCTION,
    DESCRIBE_TABLE,
    GRANT_PRIVILEGE,
    DROP_TABLE,
    DROP_MATERIALIZED_VIEW,
    DROP_VIEW,
    DROP_INDEX,
    DROP_FUNCTION,
    DROP_SOURCE,
    DROP_SINK,
    DROP_SCHEMA,
    DROP_DATABASE,
    DROP_USER,
    ALTER_TABLE,
    ALTER_SYSTEM,
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

pub trait Callback = Future<Output = Result<(), BoxedError>> + Send;
pub type BoxedCallback = Pin<Box<dyn Callback>>;

pub struct PgResponse<VS> {
    stmt_type: StatementType,
    // row count of effected row. Used for INSERT, UPDATE, DELETE, COPY, and other statements that
    // don't return rows.
    row_cnt: Option<i32>,
    notice: Option<String>,
    values_stream: Option<VS>,
    callback: Option<BoxedCallback>,
    row_desc: Vec<PgFieldDescriptor>,
}

impl<VS> std::fmt::Debug for PgResponse<VS>
where
    VS: ValuesStream,
{
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
                | StatementType::INSERT_RETURNING
                | StatementType::DELETE_RETURNING
                | StatementType::UPDATE_RETURNING
        )
    }

    pub fn is_dml(&self) -> bool {
        matches!(
            self,
            StatementType::INSERT
                | StatementType::DELETE
                | StatementType::UPDATE
                | StatementType::INSERT_RETURNING
                | StatementType::DELETE_RETURNING
                | StatementType::UPDATE_RETURNING
        )
    }

    pub fn is_query(&self) -> bool {
        matches!(
            self,
            StatementType::SELECT
                | StatementType::EXPLAIN
                | StatementType::SHOW_COMMAND
                | StatementType::DESCRIBE_TABLE
                | StatementType::INSERT_RETURNING
                | StatementType::DELETE_RETURNING
                | StatementType::UPDATE_RETURNING
        )
    }

    pub fn is_returning(&self) -> bool {
        matches!(
            self,
            StatementType::INSERT_RETURNING
                | StatementType::DELETE_RETURNING
                | StatementType::UPDATE_RETURNING
        )
    }
}

impl<VS> PgResponse<VS>
where
    VS: ValuesStream,
{
    pub fn empty_result(stmt_type: StatementType) -> Self {
        let row_cnt = if stmt_type.is_query() { None } else { Some(0) };
        Self {
            stmt_type,
            row_cnt,
            values_stream: None,
            row_desc: vec![],
            notice: None,
            callback: None,
        }
    }

    pub fn empty_result_with_notice(stmt_type: StatementType, notice: String) -> Self {
        let row_cnt = if stmt_type.is_query() { None } else { Some(0) };
        Self {
            stmt_type,
            row_cnt,
            values_stream: None,
            row_desc: vec![],
            notice: if !notice.is_empty() {
                Some(notice)
            } else {
                None
            },
            callback: None,
        }
    }

    pub fn new_for_stream(
        stmt_type: StatementType,
        row_cnt: Option<i32>,
        values_stream: VS,
        row_desc: Vec<PgFieldDescriptor>,
    ) -> Self {
        Self::new_for_stream_inner(stmt_type, row_cnt, values_stream, row_desc, None, None)
    }

    pub fn new_for_stream_extra(
        stmt_type: StatementType,
        row_cnt: Option<i32>,
        values_stream: VS,
        row_desc: Vec<PgFieldDescriptor>,
        notice: String,
        callback: impl Callback + 'static,
    ) -> Self {
        Self::new_for_stream_inner(
            stmt_type,
            row_cnt,
            values_stream,
            row_desc,
            if !notice.is_empty() {
                Some(notice)
            } else {
                None
            },
            Some(callback.boxed()),
        )
    }

    fn new_for_stream_inner(
        stmt_type: StatementType,
        row_cnt: Option<i32>,
        values_stream: VS,
        row_desc: Vec<PgFieldDescriptor>,
        notice: Option<String>,
        callback: Option<BoxedCallback>,
    ) -> Self {
        assert!(
            stmt_type.is_query() ^ row_cnt.is_some(),
            "should specify row count for command and not for query: {stmt_type}"
        );
        Self {
            stmt_type,
            row_cnt,
            values_stream: Some(values_stream),
            row_desc,
            notice,
            callback,
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

    pub fn values_stream(&mut self) -> &mut VS {
        self.values_stream.as_mut().expect("no values stream")
    }

    /// Run the callback if there is one.
    ///
    /// This should only be called after the values stream has been exhausted. Multiple calls to
    /// this function will be no-ops.
    pub async fn run_callback(&mut self) -> Result<(), PsqlError> {
        // Check if the stream is exhausted.
        if let Some(values_stream) = &mut self.values_stream {
            assert!(values_stream.next().await.is_none());
        }

        if let Some(callback) = self.callback.take() {
            callback.await.map_err(PsqlError::ExecuteError)?;
        }
        Ok(())
    }
}
