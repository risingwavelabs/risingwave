use std::ops::Bound::{Excluded, Included, Unbounded};
use std::ops::RangeBounds;
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

pub fn range_overlap<R, B>(
    search_key_range: &R,
    inclusive_start_key: &[u8],
    inclusive_end_key: &[u8],
    reverse: bool,
) -> bool
where
    R: RangeBounds<B>,
    B: AsRef<[u8]>,
{
    let (start_bound, end_bound) = if reverse {
        (search_key_range.end_bound(), search_key_range.start_bound())
    } else {
        (search_key_range.start_bound(), search_key_range.end_bound())
    };

    //        RANGE
    // TABLE
    let too_left = match start_bound {
        Included(range_start) => range_start.as_ref() > inclusive_end_key,
        Excluded(range_start) => range_start.as_ref() >= inclusive_end_key,
        Unbounded => false,
    };
    // RANGE
    //        TABLE
    let too_right = match end_bound {
        Included(range_end) => range_end.as_ref() < inclusive_start_key,
        Excluded(range_end) => range_end.as_ref() <= inclusive_start_key,
        Unbounded => false,
    };

    !too_left && !too_right
}
