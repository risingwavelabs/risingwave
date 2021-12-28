use risingwave_common::array::Row;
use risingwave_common::types::ScalarImpl;

use crate::parser::parse;
use crate::pgwire::pg_field_descriptor::{PgFieldDescriptor, TypeOid};
use crate::pgwire::pg_result::{PgResult, StatementType};
pub struct Database {}

impl Database {
    pub fn run_statement(&self, sql_stmt: &str) -> PgResult {
        // Parse sql.
        let ast = parse(sql_stmt).unwrap();
        // Hack a empty pg results. Replace it with real execution.
        PgResult::new(
            StatementType::SELECT,
            1,
            vec![Row::new(vec![Some(ScalarImpl::Utf8(format!("{:?}", ast)))])],
            vec![PgFieldDescriptor::new(
                "varchar".to_owned(),
                TypeOid::Varchar,
            )],
        )
    }
}
