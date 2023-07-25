use risingwave_storage::table::batch_table::storage_table::StorageTable;
use risingwave_storage::StateStore;

use crate::executor::backfill::upstream_table::external::ExternalStorageTable;

pub mod binlog;
pub mod external;
pub mod snapshot;

pub trait UpstreamTable {
    fn identity(&self) -> &str;
}

// pub enum UpstreamDbType {
//     MYSQL,
//     POSTGRES,
//     RISINGWAVE,
// }

#[derive(Debug, Clone)]
pub struct SchemaTableName {
    pub schema_name: String,
    pub table_name: String,
}

impl<S: StateStore> UpstreamTable for StorageTable<S> {
    fn identity(&self) -> &str {
        "StorageTable"
    }
}

impl UpstreamTable for ExternalStorageTable {
    fn identity(&self) -> &str {
        "ExternalStorageTable"
    }
}
