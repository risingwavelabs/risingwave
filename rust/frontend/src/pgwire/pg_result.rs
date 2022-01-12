use std::fmt::Formatter;

use risingwave_common::array::Row;

use crate::pgwire::pg_field_descriptor::PgFieldDescriptor;
/// Port from StatementType.java.

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
#[allow(non_camel_case_types)]
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
    CREATE_STREAM,
    DROP_TABLE,
    DROP_STREAM,
    // Introduce ORDER_BY statement type cuz Calcite unvalidated AST has SqlKind.ORDER_BY. Note
    // that Statement Type is not designed to be one to one mapping with SqlKind.
    ORDER_BY,
    SET_OPTION,
    SHOW_PARAMETERS,
    OTHER,
}

impl std::fmt::Display for StatementType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

pub struct PgResult {
    stmt_type: StatementType,
    row_cnt: i32,

    values: Vec<Row>,
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
}

impl PgResult {
    pub fn new(
        stmt_type: StatementType,
        row_cnt: i32,
        values: Vec<Row>,
        row_desc: Vec<PgFieldDescriptor>,
    ) -> Self {
        Self {
            stmt_type,
            row_cnt,
            values,
            row_desc,
        }
    }

    pub fn get_stmt_type(&self) -> StatementType {
        self.stmt_type
    }

    pub fn get_effected_rows_cnt(&self) -> i32 {
        self.row_cnt
    }

    pub fn is_query(&self) -> bool {
        self.stmt_type == StatementType::SELECT
    }

    pub fn get_row_desc(&self) -> Vec<PgFieldDescriptor> {
        self.row_desc.clone()
    }

    pub fn iter(&self) -> impl Iterator<Item = &Row> + '_ {
        self.values.iter()
    }
}

/// Helper to return a 1-row-1-col string at early stage of developement.
impl From<String> for PgResult {
    fn from(s: String) -> Self {
        use risingwave_common::types::ScalarImpl;

        use crate::pgwire::pg_field_descriptor::TypeOid;
        PgResult::new(
            StatementType::SELECT,
            1,
            vec![Row::new(vec![Some(ScalarImpl::Utf8(s))])],
            vec![PgFieldDescriptor::new(
                "varchar".to_owned(),
                TypeOid::Varchar,
            )],
        )
    }
}
