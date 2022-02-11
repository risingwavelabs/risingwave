mod current_version_id;
mod pinned_snapshot;
mod pinned_version;
mod sstable_info;
mod sstable_to_delete;
mod version;

pub use current_version_id::*;
pub use pinned_snapshot::*;
pub use pinned_version::*;
pub use sstable_to_delete::*;
pub use version::*;

use crate::storage::Transaction;

/// Column family name for hummock epoch.
pub(crate) const HUMMOCK_DEFAULT_CF_NAME: &str = "cf/hummock_default";

pub trait Transactional {
    fn upsert(&self, trx: &mut Transaction);
    fn delete(&self, trx: &mut Transaction);
}
