use std::sync::Arc;

use super::{HummockResult, Table};

pub fn bloom_filter_tables(tables: Vec<Arc<Table>>, key: &[u8]) -> HummockResult<Vec<Arc<Table>>> {
    let bf_tables = tables
        .into_iter()
        .filter(|table| !table.surely_not_have_user_key(key))
        .collect::<Vec<_>>();

    Ok(bf_tables)
}
