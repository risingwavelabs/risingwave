// Copyright 2024 RisingWave Labs
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

//! Hummock is the state store of the streaming system.

use std::ops::Bound;
use std::sync::Arc;

use bytes::Bytes;
use risingwave_hummock_sdk::key::{FullKey, TableKey, UserKeyRangeRef};
use risingwave_hummock_sdk::{HummockEpoch, *};
use risingwave_pb::hummock::SstableInfo;

pub mod block_cache;
pub use block_cache::*;

pub mod file_cache;
pub use file_cache::*;

pub mod sstable;
pub use sstable::*;

pub mod compactor;
pub mod conflict_detector;
mod error;
pub mod hummock_meta_client;
pub mod iterator;
pub mod shared_buffer;
pub mod sstable_store;
#[cfg(any(test, feature = "test"))]
pub mod test_utils;
pub mod utils;
pub use utils::MemoryLimiter;
pub mod backup_reader;
pub mod event_handler;
pub mod local_version;
pub mod observer_manager;
pub mod store;
pub use store::*;
pub mod vacuum;
mod validator;
pub mod value;
pub mod write_limiter;

pub mod block_stream;

pub use error::*;
pub use risingwave_common::cache::{CacheableEntry, LookupResult, LruCache};
pub use validator::*;
use value::*;

use self::iterator::HummockIterator;
pub use self::sstable_store::*;
use crate::mem_table::ImmutableMemtable;
use crate::monitor::StoreLocalStatistic;
use crate::store::ReadOptions;

pub async fn get_from_sstable_info(
    sstable_store_ref: SstableStoreRef,
    sstable_info: &SstableInfo,
    full_key: FullKey<&[u8]>,
    read_options: &ReadOptions,
    dist_key_hash: Option<u64>,
    local_stats: &mut StoreLocalStatistic,
) -> HummockResult<Option<(HummockValue<Bytes>, EpochWithGap)>> {
    let sstable = sstable_store_ref.sstable(sstable_info, local_stats).await?;

    // Bloom filter key is the distribution key, which is no need to be the prefix of pk, and do not
    // contain `TablePrefix` and `VnodePrefix`.
    if let Some(hash) = dist_key_hash
        && !hit_sstable_bloom_filter(
            &sstable,
            &(
                Bound::Included(full_key.user_key),
                Bound::Included(full_key.user_key),
            ),
            hash,
            local_stats,
        )
    {
        if !read_options.ignore_range_tombstone {
            let delete_epoch = get_min_delete_range_epoch_from_sstable(&sstable, full_key.user_key);
            if delete_epoch <= full_key.epoch_with_gap.pure_epoch() {
                return Ok(Some((
                    HummockValue::Delete,
                    EpochWithGap::new_from_epoch(delete_epoch),
                )));
            }
        }

        return Ok(None);
    }

    // TODO: now SstableIterator does not use prefetch through SstableIteratorReadOptions, so we
    // use default before refinement.
    let mut iter = SstableIterator::create(
        sstable,
        sstable_store_ref.clone(),
        Arc::new(SstableIteratorReadOptions::from_read_options(read_options)),
    );
    iter.seek(full_key).await?;
    // Iterator has sought passed the borders.
    if !iter.is_valid() {
        if !read_options.ignore_range_tombstone {
            let delete_epoch =
                get_min_delete_range_epoch_from_sstable(iter.sst(), full_key.user_key);
            if delete_epoch <= full_key.epoch_with_gap.pure_epoch() {
                return Ok(Some((
                    HummockValue::Delete,
                    EpochWithGap::new_from_epoch(delete_epoch),
                )));
            }
        }

        return Ok(None);
    }

    // Iterator gets us the key, we tell if it's the key we want
    // or key next to it.
    let value = if iter.key().user_key == full_key.user_key {
        Some((iter.value().to_bytes(), iter.key().epoch_with_gap))
    } else if !read_options.ignore_range_tombstone {
        let delete_epoch = get_min_delete_range_epoch_from_sstable(iter.sst(), full_key.user_key);
        if delete_epoch <= full_key.epoch_with_gap.pure_epoch() {
            Some((
                HummockValue::Delete,
                EpochWithGap::new_from_epoch(delete_epoch),
            ))
        } else {
            None
        }
    } else {
        None
    };

    iter.collect_local_statistic(local_stats);

    Ok(value)
}

pub fn hit_sstable_bloom_filter(
    sstable_info_ref: &Sstable,
    user_key_range: &UserKeyRangeRef<'_>,
    prefix_hash: u64,
    local_stats: &mut StoreLocalStatistic,
) -> bool {
    local_stats.bloom_filter_check_counts += 1;
    let may_exist = sstable_info_ref.may_match_hash(user_key_range, prefix_hash);
    if !may_exist {
        local_stats.bloom_filter_true_negative_counts += 1;
    }
    may_exist
}

/// Get `user_value` from `ImmutableMemtable`
pub fn get_from_batch(
    imm: &ImmutableMemtable,
    table_key: TableKey<&[u8]>,
    read_epoch: HummockEpoch,
    read_options: &ReadOptions,
    local_stats: &mut StoreLocalStatistic,
) -> Option<(HummockValue<Bytes>, EpochWithGap)> {
    imm.get(table_key, read_epoch, read_options).map(|v| {
        local_stats.get_shared_buffer_hit_counts += 1;
        v
    })
}
