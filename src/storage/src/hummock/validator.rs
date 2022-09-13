// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::borrow::BorrowMut;
use std::cmp;
use std::collections::HashMap;
use std::sync::Arc;

use risingwave_hummock_sdk::VersionedComparator;
use risingwave_pb::hummock::ValidationTask;

use crate::hummock::iterator::HummockIterator;
use crate::hummock::sstable::SstableIteratorReadOptions;
use crate::hummock::sstable_store::SstableStoreRef;
use crate::hummock::SstableIterator;
use crate::monitor::StoreLocalStatistic;

/// Validate SSTs in terms of Ordered, Locally unique and Globally unique.
///
/// See `src/storage/src/hummock/state_store.rs`
pub async fn validate_ssts(task: ValidationTask, sstable_store: SstableStoreRef) {
    let mut visited_keys = HashMap::new();
    let mut unused = StoreLocalStatistic::default();
    for sst in task.sst_infos {
        let mut key_counts = 0;
        let worker_id = *task
            .sst_id_to_worker_id
            .get(&sst.id)
            .expect("valid worker_id");
        tracing::debug!(
            "Validating SST {} from worker {}, epoch {}",
            sst.id,
            worker_id,
            task.epoch
        );
        let holder = match sstable_store.sstable(&sst, unused.borrow_mut()).await {
            Ok(holder) => holder,
            Err(err) => {
                // One reasonable cause is the SST has been vacuumed.
                tracing::warn!("Skip sanity check for SST {}. {}", sst.id, err);
                continue;
            }
        };

        // TODO: to use `prefetch: false` after `prefetch` be supported by
        // SstableIteratorReadOptions
        let mut iter = SstableIterator::new(
            holder,
            sstable_store.clone(),
            Arc::new(SstableIteratorReadOptions::default()),
        );
        let mut previous_key: Option<Vec<u8>> = None;
        if let Err(err) = iter.rewind().await {
            tracing::warn!("Skip sanity check for SST {}. {}", sst.id, err);
        }
        while iter.is_valid() {
            key_counts += 1;
            let current_key = iter.key().to_vec();
            // Locally unique and Globally unique
            if let Some((duplicate_sst_id, duplicate_worker_id)) =
                visited_keys.get(&current_key).cloned()
            {
                panic!("SST sanity check failed: Duplicate key {:x?} in SST {} from worker {} and SST {} from worker {}",
                       current_key,
                       sst.id,
                       worker_id,
                       duplicate_sst_id,
                       duplicate_worker_id)
            }
            visited_keys.insert(current_key.to_owned(), (sst.id, worker_id));
            // Ordered and Locally unique
            if let Some(previous_key) = previous_key.take() {
                let cmp = VersionedComparator::compare_key(&previous_key, &current_key);
                if cmp != cmp::Ordering::Less {
                    panic!(
                        "SST sanity check failed: For SST {}, expect {:x?} < {:x?}, got {:#?}",
                        sst.id, previous_key, current_key, cmp
                    )
                }
            }
            previous_key = Some(current_key);
            if let Err(err) = iter.next().await {
                tracing::warn!("Skip remaining sanity check for SST {}. {}", sst.id, err);
                break;
            }
        }
        tracing::debug!(
            "Validated {} keys for SST {},  epoch {}",
            key_counts,
            sst.id,
            task.epoch
        );
        iter.collect_local_statistic(&mut unused);
        unused.ignore();
    }
}
