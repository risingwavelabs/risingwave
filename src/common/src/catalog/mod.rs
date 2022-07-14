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
pub use schema::{test_utils as schema_test_utils, Field, FieldVerboseDisplay, Schema};

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
    schema_id: i32,
}

impl SchemaId {
    pub fn new(schema_id: i32) -> Self {
        SchemaId { schema_id }
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

// TODO: TableOption is duplicated with the properties in table catalog, We can refactor later to
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
