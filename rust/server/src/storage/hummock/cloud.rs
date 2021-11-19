use crate::storage::hummock::{HummockOptions, Table};

/// Upload table to remote object storage and return the URL
pub fn upload(_table: Table, _options: &HummockOptions) -> Box<[u8]> {
    todo!()
}
