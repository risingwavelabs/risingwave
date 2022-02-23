pub mod schema;

pub use schema::*;

pub enum CatalogId {
    DatabaseId(DatabaseId),
    SchemaId(SchemaId),
    TableId(TableId),
}

#[derive(Clone, Debug, Default, Hash, PartialOrd, PartialEq, Eq)]
pub struct DatabaseId {
    database_id: i32,
}

impl DatabaseId {
    pub fn new(database_id: i32) -> Self {
        DatabaseId { database_id }
    }
}

#[derive(Clone, Debug, Default, Hash, PartialOrd, PartialEq, Eq)]
pub struct SchemaId {
    database_ref_id: DatabaseId,
    schema_id: i32,
}

impl SchemaId {
    pub fn new(database_ref_id: DatabaseId, schema_id: i32) -> Self {
        SchemaId {
            database_ref_id,
            schema_id,
        }
    }
}

#[derive(Clone, Debug, Default, Hash, PartialOrd, PartialEq, Eq)]
pub struct TableId {
    pub table_id: u32,
}
impl TableId {
    pub fn new(table_id: u32) -> Self {
        TableId { table_id }
    }

    pub fn table_id(&self) -> u32 {
        self.table_id
    }
}

// TODO: replace boilerplate code
impl From<&Option<risingwave_pb::plan::DatabaseRefId>> for DatabaseId {
    fn from(option: &Option<risingwave_pb::plan::DatabaseRefId>) -> Self {
        match option {
            Some(pb) => DatabaseId {
                database_id: pb.database_id,
            },
            None => DatabaseId {
                database_id: Default::default(),
            },
        }
    }
}

// TODO: replace boilerplate code
impl From<&Option<risingwave_pb::plan::SchemaRefId>> for SchemaId {
    fn from(option: &Option<risingwave_pb::plan::SchemaRefId>) -> Self {
        match option {
            Some(pb) => SchemaId {
                database_ref_id: DatabaseId::from(&pb.database_ref_id),
                schema_id: pb.schema_id,
            },
            None => {
                let pb = risingwave_pb::plan::SchemaRefId::default();
                SchemaId {
                    database_ref_id: DatabaseId::from(&pb.database_ref_id),
                    schema_id: pb.schema_id,
                }
            }
        }
    }
}

// TODO: replace boilerplate code
impl From<&Option<risingwave_pb::plan::TableRefId>> for TableId {
    fn from(option: &Option<risingwave_pb::plan::TableRefId>) -> Self {
        match option {
            Some(pb) => TableId {
                table_id: pb.table_id as u32,
            },
            None => {
                let pb = risingwave_pb::plan::TableRefId::default();
                TableId {
                    table_id: pb.table_id as u32,
                }
            }
        }
    }
}

// TODO: replace boilerplate code
impl From<&DatabaseId> for risingwave_pb::plan::DatabaseRefId {
    fn from(database_id: &DatabaseId) -> Self {
        risingwave_pb::plan::DatabaseRefId {
            database_id: database_id.database_id,
        }
    }
}

// TODO: replace boilerplate code
impl From<&SchemaId> for risingwave_pb::plan::SchemaRefId {
    fn from(schema_id: &SchemaId) -> Self {
        risingwave_pb::plan::SchemaRefId {
            database_ref_id: Some(risingwave_pb::plan::DatabaseRefId::from(
                &schema_id.database_ref_id,
            )),
            schema_id: schema_id.schema_id,
        }
    }
}

// TODO: replace boilerplate code
impl From<&TableId> for risingwave_pb::plan::TableRefId {
    fn from(table_id: &TableId) -> Self {
        risingwave_pb::plan::TableRefId {
            schema_ref_id: None,
            table_id: table_id.table_id as i32,
        }
    }
}

#[cfg(test)]
mod tests {
    use risingwave_pb::plan::{DatabaseRefId, SchemaRefId, TableRefId};

    use crate::catalog::{DatabaseId, SchemaId, TableId};

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
