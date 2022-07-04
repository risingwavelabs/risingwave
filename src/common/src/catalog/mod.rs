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

mod column;
mod internal_table;
mod physical_table;
mod schema;
pub mod test_utils;

use core::fmt;
use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
pub use column::*;
pub use internal_table::*;
pub use physical_table::*;
pub use schema::{test_utils as schema_test_utils, Field, Schema};

use crate::array::Row;
pub use crate::config::constant::hummock;
use crate::error::Result;

pub const DEFAULT_DATABASE_NAME: &str = "dev";
pub const DEFAULT_SCHEMA_NAME: &str = "public";
pub const PG_CATALOG_SCHEMA_NAME: &str = "pg_catalog";
pub const RESERVED_PG_SCHEMA_PREFIX: &str = "pg_";
pub const DEFAULT_SUPPER_USER: &str = "root";
// This is for compatibility with customized utils for PostgreSQL.
pub const DEFAULT_SUPPER_USER_FOR_PG: &str = "postgres";

pub const RESERVED_PG_CATALOG_TABLE_ID: i32 = 1000;

/// The local system catalog reader in the frontend node.
#[async_trait]
pub trait SysCatalogReader: Sync + Send + 'static {
    async fn read_table(&self, table_name: &str) -> Result<Vec<Row>>;
}

pub type SysCatalogReaderRef = Arc<dyn SysCatalogReader>;

pub type CatalogVersion = u64;

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

    pub fn placeholder() -> i32 {
        i32::MAX - 1
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

    pub fn placeholder() -> i32 {
        i32::MAX - 1
    }
}

#[derive(Clone, Copy, Debug, Default, Hash, PartialOrd, PartialEq, Eq)]
pub struct TableId {
    pub table_id: u32,
}

impl TableId {
    pub const fn new(table_id: u32) -> Self {
        TableId { table_id }
    }

    /// Sometimes the id field is filled later, we use this value for better debugging.
    pub const fn placeholder() -> Self {
        TableId {
            table_id: u32::MAX - 1,
        }
    }

    pub fn table_id(&self) -> u32 {
        self.table_id
    }
}

impl From<u32> for TableId {
    fn from(id: u32) -> Self {
        Self::new(id)
    }
}
impl From<TableId> for u32 {
    fn from(id: TableId) -> Self {
        id.table_id
    }
}

impl fmt::Display for TableId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.table_id,)
    }
}

// TODO: replace boilerplate code
impl From<&Option<risingwave_pb::plan_common::DatabaseRefId>> for DatabaseId {
    fn from(option: &Option<risingwave_pb::plan_common::DatabaseRefId>) -> Self {
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
impl From<&Option<risingwave_pb::plan_common::SchemaRefId>> for SchemaId {
    fn from(option: &Option<risingwave_pb::plan_common::SchemaRefId>) -> Self {
        match option {
            Some(pb) => SchemaId {
                database_ref_id: DatabaseId::from(&pb.database_ref_id),
                schema_id: pb.schema_id,
            },
            None => {
                let pb = risingwave_pb::plan_common::SchemaRefId::default();
                SchemaId {
                    database_ref_id: DatabaseId::from(&pb.database_ref_id),
                    schema_id: pb.schema_id,
                }
            }
        }
    }
}

// TODO: replace boilerplate code
impl From<&Option<risingwave_pb::plan_common::TableRefId>> for TableId {
    fn from(option: &Option<risingwave_pb::plan_common::TableRefId>) -> Self {
        match option {
            Some(pb) => TableId {
                table_id: pb.table_id as u32,
            },
            None => {
                let pb = risingwave_pb::plan_common::TableRefId::default();
                TableId {
                    table_id: pb.table_id as u32,
                }
            }
        }
    }
}

// TODO: replace boilerplate code
impl From<&DatabaseId> for risingwave_pb::plan_common::DatabaseRefId {
    fn from(database_id: &DatabaseId) -> Self {
        risingwave_pb::plan_common::DatabaseRefId {
            database_id: database_id.database_id,
        }
    }
}

// TODO: replace boilerplate code
impl From<&SchemaId> for risingwave_pb::plan_common::SchemaRefId {
    fn from(schema_id: &SchemaId) -> Self {
        risingwave_pb::plan_common::SchemaRefId {
            database_ref_id: Some(risingwave_pb::plan_common::DatabaseRefId::from(
                &schema_id.database_ref_id,
            )),
            schema_id: schema_id.schema_id,
        }
    }
}

// TODO: replace boilerplate code
impl From<&TableId> for risingwave_pb::plan_common::TableRefId {
    fn from(table_id: &TableId) -> Self {
        risingwave_pb::plan_common::TableRefId {
            schema_ref_id: None,
            table_id: table_id.table_id as i32,
        }
    }
}

// TODO: TableOption is deplicated with the properties in table catalog, We can refactor later to
// directly fetch such options from catalog when creating compaction jobs.
#[derive(Clone, Debug, PartialEq, Default, Copy)]
pub struct TableOption {
    pub ttl: Option<u32>, // second
}

impl From<&risingwave_pb::hummock::TableOption> for TableOption {
    fn from(table_option: &risingwave_pb::hummock::TableOption) -> Self {
        let ttl = if table_option.ttl == hummock::TABLE_OPTION_DUMMY_TTL {
            None
        } else {
            Some(table_option.ttl)
        };

        Self { ttl }
    }
}

impl From<&TableOption> for risingwave_pb::hummock::TableOption {
    fn from(table_option: &TableOption) -> Self {
        Self {
            ttl: table_option.ttl.unwrap_or(hummock::TABLE_OPTION_DUMMY_TTL),
        }
    }
}

impl TableOption {
    pub fn build_table_option(table_properties: &HashMap<String, String>) -> Self {
        // now we only support ttl for TableOption
        let mut result = TableOption::default();
        match table_properties.get(hummock::PROPERTIES_TTL_KEY) {
            Some(ttl_string) => {
                match ttl_string.trim().parse::<u32>() {
                    Ok(ttl_u32) => result.ttl = Some(ttl_u32),
                    Err(e) => {
                        tracing::info!(
                            "build_table_option parse option ttl_string {} fail {}",
                            ttl_string,
                            e
                        );
                        result.ttl = None;
                    }
                };
            }

            None => {}
        }

        result
    }
}

#[cfg(test)]
mod tests {
    use risingwave_pb::plan_common::{DatabaseRefId, SchemaRefId, TableRefId};

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
            table_id: generate_table_id_pb().table_id as u32,
        };

        let expected_table_id = TableId { table_id: 67 };

        assert_eq!(expected_table_id, table_id);
    }
}
