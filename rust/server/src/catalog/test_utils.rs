use crate::catalog::{DatabaseId, SchemaId, TableId};

pub fn mock_table_id() -> TableId {
    TableId::new(SchemaId::new(DatabaseId::new(0), 0), 0)
}
