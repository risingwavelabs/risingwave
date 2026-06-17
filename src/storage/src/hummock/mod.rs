// Copyright 2022 RisingWave Labs
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
use risingwave_hummock_sdk::sstable_info::SstableInfo;
use risingwave_hummock_sdk::{HummockEpoch, *};

pub mod block_cache;
pub use block_cache::*;

pub mod sstable;
pub use sstable::*;

pub mod compactor;
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
mod validator;
pub mod value;
pub mod write_limiter;

pub mod recent_filter;
pub use recent_filter::*;

pub mod block_stream;
mod time_travel_version_cache;

pub(crate) mod vector;

mod object_id_manager;
pub use error::*;
pub use object_id_manager::*;
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
) -> HummockResult<Option<impl HummockIterator>> {
    let sstable = sstable_store_ref
        .meta_index(sstable_info, local_stats)
        .await?;
    let index = &sstable.index;

    if let Some(hash) = dist_key_hash {
        let user_key_range = (
            Bound::Included(full_key.user_key),
            Bound::Included(full_key.user_key),
        );
        let mut may_match = false;
        let mut checked_filter = false;
        for desc in index.filter_candidate_shards_by_user_key(full_key) {
            let shard = sstable_store_ref
                .get_meta_shard_holder(sstable.id, index.filter_type, desc, local_stats)
                .await?;
            checked_filter = true;
            if shard.may_match(&user_key_range, hash) {
                local_stats.partitioned_meta_shard_filter_positive_counts += 1;
                may_match = true;
                break;
            }

            local_stats.partitioned_meta_shard_filter_negative_counts += 1;
        }

        if checked_filter {
            local_stats.bloom_filter_check_counts += 1;
        }
        if !may_match {
            if checked_filter {
                local_stats.bloom_filter_true_negative_counts += 1;
            }
            return Ok(None);
        }
    }

    let mut iter = SstableIterator::create(
        sstable,
        sstable_store_ref.clone(),
        Arc::new(SstableIteratorReadOptions::from_read_options(read_options)),
        sstable_info,
    );
    iter.seek(full_key).await?;
    // Iterator has sought passed the borders.
    if !iter.is_valid() {
        return Ok(None);
    }

    iter.collect_local_statistic(local_stats);

    // Iterator gets us the key, we tell if it's the key we want
    // or key next to it.
    let value = if iter.key().user_key == full_key.user_key {
        Some(iter)
    } else {
        None
    };

    Ok(value)
}

pub async fn hit_sstable_filter_with_partitioned_meta(
    sstable_store_ref: &SstableStoreRef,
    sstable_ref: &PartitionedSstableMeta,
    user_key_range: &UserKeyRangeRef<'_>,
    prefix_hash: u64,
    local_stats: &mut StoreLocalStatistic,
) -> HummockResult<bool> {
    let index = &sstable_ref.index;
    let mut checked_filter = false;
    let mut may_match = false;
    for desc in index.filter_candidate_shards_by_user_key_range(user_key_range) {
        let shard = sstable_store_ref
            .get_meta_shard_holder(sstable_ref.id, index.filter_type, desc, local_stats)
            .await?;
        checked_filter = true;

        if shard.may_match(user_key_range, prefix_hash) {
            local_stats.partitioned_meta_shard_filter_positive_counts += 1;
            may_match = true;
            break;
        }

        local_stats.partitioned_meta_shard_filter_negative_counts += 1;
    }

    if checked_filter {
        local_stats.bloom_filter_check_counts += 1;
        if !may_match {
            local_stats.bloom_filter_true_negative_counts += 1;
        }
    }
    Ok(may_match)
}

/// Get `user_value` from `ImmutableMemtable`
pub fn get_from_batch<'a>(
    imm: &'a ImmutableMemtable,
    table_key: TableKey<&[u8]>,
    read_epoch: HummockEpoch,
    read_options: &ReadOptions,
    local_stats: &mut StoreLocalStatistic,
) -> Option<(HummockValue<&'a Bytes>, EpochWithGap)> {
    imm.get(table_key, read_epoch, read_options).inspect(|_| {
        local_stats.get_shared_buffer_hit_counts += 1;
    })
}
