pub mod test_utils;

pub mod schema;
pub use schema::*;

use pb_convert_derive::FromProtobuf;

#[derive(Clone, Debug, Hash, PartialOrd, PartialEq, Eq, FromProtobuf)]
#[pb_convert(pb_type = "risingwave_proto::plan::DatabaseRefId")]
pub struct DatabaseId {
    database_id: i32,
}

impl DatabaseId {
    fn new(database_id: i32) -> Self {
        DatabaseId { database_id }
    }
}

#[derive(Clone, Debug, Hash, PartialOrd, PartialEq, Eq, FromProtobuf)]
#[pb_convert(pb_type = "risingwave_proto::plan::SchemaRefId")]
pub struct SchemaId {
    database_ref_id: DatabaseId,
    schema_id: i32,
}

impl SchemaId {
    fn new(database_ref_id: DatabaseId, schema_id: i32) -> Self {
        SchemaId {
            database_ref_id,
            schema_id,
        }
    }
}

#[derive(Clone, Debug, Hash, PartialOrd, PartialEq, Eq, FromProtobuf)]
#[pb_convert(pb_type = "risingwave_proto::plan::TableRefId")]
pub struct TableId {
    schema_ref_id: SchemaId,
    table_id: i32,
}

impl TableId {
    fn new(schema_ref_id: SchemaId, table_id: i32) -> Self {
        TableId {
            schema_ref_id,
            table_id,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::catalog::{DatabaseId, SchemaId, TableId};
    use pb_convert::FromProtobuf;
    use risingwave_proto::plan::{DatabaseRefId, SchemaRefId, TableRefId};

    fn generate_database_id_proto() -> DatabaseRefId {
        let mut database_id_proto = DatabaseRefId::new();
        database_id_proto.set_database_id(32);

        database_id_proto
    }

    fn generate_schema_id_proto() -> SchemaRefId {
        let mut schema_id_proto = SchemaRefId::new();
        schema_id_proto.set_database_ref_id(generate_database_id_proto());
        schema_id_proto.set_schema_id(48);
        schema_id_proto
    }

    fn generate_table_id_proto() -> TableRefId {
        let mut table_id_proto = TableRefId::new();
        table_id_proto.set_schema_ref_id(generate_schema_id_proto());
        table_id_proto.set_table_id(67);
        table_id_proto
    }

    #[test]
    fn test_database_id_from_pb() {
        let db_id_proto = generate_database_id_proto();
        let database_id =
            DatabaseId::from_protobuf(&db_id_proto).expect("Falied to convert database id");

        assert_eq!(32, database_id.database_id);
    }

    #[test]
    fn test_schema_id_from_pb() {
        let schema_id_proto = generate_schema_id_proto();
        let schema_id =
            SchemaId::from_protobuf(&schema_id_proto).expect("Failed to convert schema id");

        let expected_schema_id = SchemaId {
            database_ref_id: DatabaseId { database_id: 32 },
            schema_id: 48,
        };

        assert_eq!(expected_schema_id, schema_id);
    }

    #[test]
    fn test_table_id_from_pb() {
        let table_id_proto = generate_table_id_proto();
        let table_id = TableId::from_protobuf(&table_id_proto).expect("Failed to convert table id");

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
