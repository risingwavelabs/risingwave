pub mod test_utils;

pub mod schema;

pub use schema::*;

use pb_convert_derive::FromProtobuf;

#[derive(Clone, Debug, Default, Hash, PartialOrd, PartialEq, Eq, FromProtobuf)]
#[pb_convert(pb_type = "risingwave_proto::plan::DatabaseRefId")]
pub struct DatabaseId {
    database_id: u64,
}

impl DatabaseId {
    pub fn new(database_id: u64) -> Self {
        DatabaseId { database_id }
    }
}

#[derive(Clone, Debug, Default, Hash, PartialOrd, PartialEq, Eq, FromProtobuf)]
#[pb_convert(pb_type = "risingwave_proto::plan::SchemaRefId")]
pub struct SchemaId {
    database_ref_id: DatabaseId,
    schema_id: u64,
}

impl SchemaId {
    pub fn new(database_ref_id: DatabaseId, schema_id: u64) -> Self {
        SchemaId {
            database_ref_id,
            schema_id,
        }
    }
}

#[derive(Clone, Debug, Default, Hash, PartialOrd, PartialEq, Eq, FromProtobuf)]
#[pb_convert(pb_type = "risingwave_proto::plan::TableRefId")]
pub struct TableId {
    schema_ref_id: SchemaId,
    table_id: u64,
}

impl TableId {
    pub fn new(schema_ref_id: SchemaId, table_id: u64) -> Self {
        TableId {
            schema_ref_id,
            table_id,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::catalog::{DatabaseId, SchemaId, TableId};
    use risingwave_pb::plan::{DatabaseRefId, SchemaRefId, TableRefId};

    fn generate_database_id_pb() -> DatabaseRefId {
        DatabaseRefId { database_id: 32 }
    }

    fn generate_schema_id_pb() -> SchemaRefId {
        SchemaRefId {
            schema_id: 48,
            ..Default::default()
        }
    }

    fn generate_table_id_pb() -> TableRefId {
        TableRefId {
            schema_ref_id: None,
            table_id: 67,
        }
    }

    #[test]
    fn test_database_id_from_pb() {
        let database_id = generate_database_id_pb();
        assert_eq!(32, database_id.database_id);
    }

    #[test]
    fn test_schema_id_from_pb() {
        let schema_id = SchemaId {
            database_ref_id: DatabaseId {
                database_id: generate_database_id_pb().database_id,
            },
            schema_id: generate_schema_id_pb().schema_id,
        };

        let expected_schema_id = SchemaId {
            database_ref_id: DatabaseId { database_id: 32 },
            schema_id: 48,
        };

        assert_eq!(expected_schema_id, schema_id);
    }

    #[test]
    fn test_table_id_from_pb() {
        let table_id: TableId = TableId {
            schema_ref_id: SchemaId {
                database_ref_id: DatabaseId {
                    database_id: generate_database_id_pb().database_id,
                },
                schema_id: generate_schema_id_pb().schema_id,
            },
            table_id: generate_table_id_pb().table_id,
        };

        let expected_table_id = TableId {
            schema_ref_id: SchemaId {
                database_ref_id: DatabaseId { database_id: 32 },
                schema_id: 48,
            },
            table_id: 67,
        };

        assert_eq!(expected_table_id, table_id);
    }
}
