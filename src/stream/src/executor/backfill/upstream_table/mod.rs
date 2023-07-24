use risingwave_storage::table::batch_table::storage_table::StorageTable;
use risingwave_storage::StateStore;

use crate::executor::backfill::external_table::ExternalStorageTable;

pub mod binlog;
pub mod snapshot;

pub trait UpstreamTable {
    fn identity(&self) -> &str;

    fn upstream_db_type(&self) -> UpstreamDbType;
}

pub enum UpstreamDbType {
    MYSQL,
    POSTGRES,
    RISINGWAVE,
}

#[derive(Debug, Clone)]
pub struct SchemaTableName {
    pub schema_name: String,
    pub table_name: String,
}

impl<S: StateStore> UpstreamTable for StorageTable<S> {
    fn identity(&self) -> &str {
        "StorageTable"
    }

    fn upstream_db_type(&self) -> UpstreamDbType {
        UpstreamDbType::RISINGWAVE
    }
}

impl UpstreamTable for ExternalStorageTable {
    fn identity(&self) -> &str {
        "ExternalStorageTable"
    }

    fn upstream_db_type(&self) -> &UpstreamDbType {
        self.db_type()
    }
}
