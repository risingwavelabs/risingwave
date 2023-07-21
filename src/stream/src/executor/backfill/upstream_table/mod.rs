use risingwave_storage::table::batch_table::storage_table::StorageTable;
use risingwave_storage::StateStore;

use crate::executor::backfill::external_table::ExternalStorageTable;

pub mod binlog;
pub mod snapshot;

pub trait UpstreamTable {
    fn identity(&self) -> &str;
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
