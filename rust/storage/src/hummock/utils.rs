use std::sync::Arc;

use super::{HummockResult, SSTable};

pub fn bloom_filter_sstables(
    tables: Vec<Arc<SSTable>>,
    key: &[u8],
) -> HummockResult<Vec<Arc<SSTable>>> {
    let bf_tables = tables
        .into_iter()
        .filter(|table| !table.surely_not_have_user_key(key))
        .collect::<Vec<_>>();

    Ok(bf_tables)
}
