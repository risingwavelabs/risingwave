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

mod column;
mod internal_table;
mod physical_table;
mod schema;
pub mod test_utils;

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
pub use column::*;
pub use internal_table::*;
use parse_display::Display;
pub use physical_table::*;
use risingwave_pb::catalog::HandleConflictBehavior as PbHandleConflictBehavior;
pub use schema::{test_utils as schema_test_utils, Field, FieldDisplay, Schema};

pub use crate::constants::hummock;
use crate::error::Result;
use crate::row::OwnedRow;
use crate::types::DataType;

/// The global version of the catalog.
pub type CatalogVersion = u64;

/// The version number of the per-table catalog.
pub type TableVersionId = u64;
/// The default version ID for a new table.
pub const INITIAL_TABLE_VERSION_ID: u64 = 0;

pub const DEFAULT_DATABASE_NAME: &str = "dev";
pub const DEFAULT_SCHEMA_NAME: &str = "public";
pub const PG_CATALOG_SCHEMA_NAME: &str = "pg_catalog";
pub const INFORMATION_SCHEMA_SCHEMA_NAME: &str = "information_schema";
pub const RW_CATALOG_SCHEMA_NAME: &str = "rw_catalog";
pub const RESERVED_PG_SCHEMA_PREFIX: &str = "pg_";
pub const DEFAULT_SUPER_USER: &str = "root";
pub const DEFAULT_SUPER_USER_ID: u32 = 1;
// This is for compatibility with customized utils for PostgreSQL.
pub const DEFAULT_SUPER_USER_FOR_PG: &str = "postgres";
pub const DEFAULT_SUPER_USER_FOR_PG_ID: u32 = 2;

pub const NON_RESERVED_USER_ID: i32 = 11;
pub const NON_RESERVED_PG_CATALOG_TABLE_ID: i32 = 1001;

pub const SYSTEM_SCHEMAS: [&str; 3] = [
    PG_CATALOG_SCHEMA_NAME,
    INFORMATION_SCHEMA_SCHEMA_NAME,
    RW_CATALOG_SCHEMA_NAME,
];

pub const ROWID_PREFIX: &str = "_row_id";

pub fn row_id_column_name() -> String {
    ROWID_PREFIX.to_string()
}

pub fn is_row_id_column_name(name: &str) -> bool {
    name.starts_with(ROWID_PREFIX)
}

/// The column ID preserved for the row ID column.
pub const ROW_ID_COLUMN_ID: ColumnId = ColumnId::new(0);

/// The column ID offset for user-defined columns.
///
/// All IDs of user-defined columns must be greater or equal to this value.
pub const USER_COLUMN_ID_OFFSET: i32 = ROW_ID_COLUMN_ID.next().get_id();

/// Creates a row ID column (for implicit primary key). It'll always have the ID `0` for now.
pub fn row_id_column_desc() -> ColumnDesc {
    ColumnDesc {
        data_type: DataType::Serial,
        column_id: ROW_ID_COLUMN_ID,
        name: row_id_column_name(),
        field_descs: vec![],
        type_name: "".to_string(),
        generated_column: None,
    }
}

/// The local system catalog reader in the frontend node.
#[async_trait]
pub trait SysCatalogReader: Sync + Send + 'static {
    async fn read_table(&self, table_id: &TableId) -> Result<Vec<OwnedRow>>;
}

pub type SysCatalogReaderRef = Arc<dyn SysCatalogReader>;

#[derive(Clone, Debug, Default, Display, Hash, PartialOrd, PartialEq, Eq)]
#[display("{database_id}")]
pub struct DatabaseId {
    pub database_id: u32,
}

impl DatabaseId {
    pub fn new(database_id: u32) -> Self {
        DatabaseId { database_id }
    }

    pub fn placeholder() -> Self {
        DatabaseId {
            database_id: u32::MAX - 1,
        }
    }
}

impl From<u32> for DatabaseId {
    fn from(id: u32) -> Self {
        Self::new(id)
    }
}

impl From<&u32> for DatabaseId {
    fn from(id: &u32) -> Self {
        Self::new(*id)
    }
}

impl From<DatabaseId> for u32 {
    fn from(id: DatabaseId) -> Self {
        id.database_id
    }
}

#[derive(Clone, Debug, Default, Display, Hash, PartialOrd, PartialEq, Eq)]
#[display("{schema_id}")]
pub struct SchemaId {
    pub schema_id: u32,
}

impl SchemaId {
    pub fn new(schema_id: u32) -> Self {
        SchemaId { schema_id }
    }

    pub fn placeholder() -> Self {
        SchemaId {
            schema_id: u32::MAX - 1,
        }
    }
}

impl From<u32> for SchemaId {
    fn from(id: u32) -> Self {
        Self::new(id)
    }
}

impl From<&u32> for SchemaId {
    fn from(id: &u32) -> Self {
        Self::new(*id)
    }
}

impl From<SchemaId> for u32 {
    fn from(id: SchemaId) -> Self {
        id.schema_id
    }
}

#[derive(Clone, Copy, Debug, Display, Default, Hash, PartialOrd, PartialEq, Eq, Ord)]
#[display("{table_id}")]
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

impl From<&u32> for TableId {
    fn from(id: &u32) -> Self {
        Self::new(*id)
    }
}

impl From<TableId> for u32 {
    fn from(id: TableId) -> Self {
        id.table_id
    }
}

#[derive(Clone, Debug, PartialEq, Default, Copy)]
pub struct TableOption {
    pub retention_seconds: Option<u32>, // second
}

impl From<&risingwave_pb::hummock::TableOption> for TableOption {
    fn from(table_option: &risingwave_pb::hummock::TableOption) -> Self {
        let retention_seconds =
            if table_option.retention_seconds == hummock::TABLE_OPTION_DUMMY_RETENTION_SECOND {
                None
            } else {
                Some(table_option.retention_seconds)
            };

        Self { retention_seconds }
    }
}

impl From<&TableOption> for risingwave_pb::hummock::TableOption {
    fn from(table_option: &TableOption) -> Self {
        Self {
            retention_seconds: table_option
                .retention_seconds
                .unwrap_or(hummock::TABLE_OPTION_DUMMY_RETENTION_SECOND),
        }
    }
}

impl TableOption {
    pub fn build_table_option(table_properties: &HashMap<String, String>) -> Self {
        // now we only support ttl for TableOption
        let mut result = TableOption::default();
        if let Some(ttl_string) = table_properties.get(hummock::PROPERTIES_RETENTION_SECOND_KEY) {
            match ttl_string.trim().parse::<u32>() {
                Ok(retention_seconds_u32) => result.retention_seconds = Some(retention_seconds_u32),
                Err(e) => {
                    tracing::info!(
                        "build_table_option parse option ttl_string {} fail {}",
                        ttl_string,
                        e
                    );
                    result.retention_seconds = None;
                }
            };
        }

        result
    }
}

#[derive(Clone, Copy, Debug, Display, Default, Hash, PartialOrd, PartialEq, Eq)]
#[display("{index_id}")]
pub struct IndexId {
    pub index_id: u32,
}

impl IndexId {
    pub const fn new(index_id: u32) -> Self {
        IndexId { index_id }
    }

    /// Sometimes the id field is filled later, we use this value for better debugging.
    pub const fn placeholder() -> Self {
        IndexId {
            index_id: u32::MAX - 1,
        }
    }

    pub fn index_id(&self) -> u32 {
        self.index_id
    }
}

impl From<u32> for IndexId {
    fn from(id: u32) -> Self {
        Self::new(id)
    }
}
impl From<IndexId> for u32 {
    fn from(id: IndexId) -> Self {
        id.index_id
    }
}

#[derive(Clone, Copy, Debug, Display, Default, Hash, PartialOrd, PartialEq, Eq, Ord)]
pub struct FunctionId(pub u32);

impl FunctionId {
    pub const fn new(id: u32) -> Self {
        FunctionId(id)
    }

    pub const fn placeholder() -> Self {
        FunctionId(u32::MAX - 1)
    }

    pub fn function_id(&self) -> u32 {
        self.0
    }
}

impl From<u32> for FunctionId {
    fn from(id: u32) -> Self {
        Self::new(id)
    }
}

impl From<&u32> for FunctionId {
    fn from(id: &u32) -> Self {
        Self::new(*id)
    }
}

impl From<FunctionId> for u32 {
    fn from(id: FunctionId) -> Self {
        id.0
    }
}

#[derive(Clone, Copy, Debug, Display, Default, Hash, PartialOrd, PartialEq, Eq, Ord)]
#[display("{user_id}")]
pub struct UserId {
    pub user_id: u32,
}

impl UserId {
    pub const fn new(user_id: u32) -> Self {
        UserId { user_id }
    }

    pub const fn placeholder() -> Self {
        UserId {
            user_id: u32::MAX - 1,
        }
    }
}

impl From<u32> for UserId {
    fn from(id: u32) -> Self {
        Self::new(id)
    }
}

impl From<&u32> for UserId {
    fn from(id: &u32) -> Self {
        Self::new(*id)
    }
}

impl From<UserId> for u32 {
    fn from(id: UserId) -> Self {
        id.user_id
    }
}

#[derive(Clone, Debug)]
#[cfg_attr(test, derive(PartialEq))]
pub enum ConflictBehavior {
    NoCheck,
    OverWrite,
    IgnoreConflict,
}

impl ConflictBehavior {
    pub fn from_protobuf(tb_conflict_behavior: &PbHandleConflictBehavior) -> Self {
        match tb_conflict_behavior {
            PbHandleConflictBehavior::Overwrite => ConflictBehavior::OverWrite,
            PbHandleConflictBehavior::Ignore => ConflictBehavior::IgnoreConflict,
            PbHandleConflictBehavior::NoCheck => ConflictBehavior::NoCheck,
            _ => unreachable!(),
        }
    }

    pub fn to_protobuf(&self) -> PbHandleConflictBehavior {
        match self {
            ConflictBehavior::NoCheck => PbHandleConflictBehavior::NoCheck,
            ConflictBehavior::OverWrite => PbHandleConflictBehavior::Overwrite,
            ConflictBehavior::IgnoreConflict => PbHandleConflictBehavior::Ignore,
        }
    }
}
