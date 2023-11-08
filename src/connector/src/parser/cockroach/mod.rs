mod json_parser;
pub use json_parser::*;

use super::unified::{Access, AccessError, ChangeEvent, ChangeEventOperation};

// Cockroach supports 4 envelope formats, wrapped, bare, key_only, and row.
// We only support `wrapped` and `bare` for now, as they ensure all information we need for CDC.
// Both formats encode all fields in the Kafka value, so we don't need to access the key.
// For all operations, including INSERT, DELETE, and UPDATE, we only access the `after` field.

const AFTER: &str = "after";

pub struct CockroachChangeEvent<A>(A);

impl<A> CockroachChangeEvent<A> {
    pub fn new(accessor: A) -> Self {
        Self(accessor)
    }
}

impl<A> ChangeEvent for CockroachChangeEvent<A>
where
    A: Access,
{
    fn access_field(
        &self,
        name: &str,
        type_expected: &risingwave_common::types::DataType,
    ) -> super::AccessResult {
        self.0.access(&[AFTER, name], Some(type_expected))
    }

    fn op(&self) -> Result<ChangeEventOperation, AccessError> {
        let op = if self.0.access(&[AFTER], None)?.is_some() {
            ChangeEventOperation::Upsert
        } else {
            // Option::None => Null, indicates DELETE.
            ChangeEventOperation::Delete
        };
        Ok(op)
    }
}
