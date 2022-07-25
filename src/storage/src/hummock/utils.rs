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

use std::cmp::Ordering;
use std::ops::Bound::{Excluded, Included, Unbounded};
use std::ops::RangeBounds;
use std::sync::atomic::{AtomicU64, AtomicUsize};
use std::sync::Arc;

use risingwave_hummock_sdk::key::user_key;
use risingwave_pb::hummock::{Level, SstableInfo};
use tokio::sync::Notify;

use super::{HummockError, HummockResult};

pub fn range_overlap<R, B>(
    search_key_range: &R,
    inclusive_start_key: &[u8],
    inclusive_end_key: &[u8],
) -> bool
where
    R: RangeBounds<B>,
    B: AsRef<[u8]>,
{
    let (start_bound, end_bound) = (search_key_range.start_bound(), search_key_range.end_bound());

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

pub fn validate_epoch(safe_epoch: u64, epoch: u64) -> HummockResult<()> {
    if epoch < safe_epoch {
        return Err(HummockError::expired_epoch(safe_epoch, epoch));
    }

    Ok(())
}

pub fn validate_table_key_range(levels: &[Level]) -> HummockResult<()> {
    for l in levels {
        for t in &l.table_infos {
            if t.key_range.is_none() {
                return Err(HummockError::meta_error(format!(
                    "key_range in table [{}] is none",
                    t.id
                )));
            }
        }
    }
    Ok(())
}

pub fn filter_single_sst<R, B>(info: &SstableInfo, key_range: &R) -> bool
where
    R: RangeBounds<B>,
    B: AsRef<[u8]>,
{
    let table_range = info.key_range.as_ref().unwrap();
    let table_start = user_key(table_range.left.as_slice());
    let table_end = user_key(table_range.right.as_slice());
    range_overlap(key_range, table_start, table_end)
}

/// Prune SSTs that does not overlap with a specific key range or does not overlap with a specific
/// vnode set. Returns the sst ids after pruning
pub fn prune_ssts<'a, R, B>(
    ssts: impl Iterator<Item = &'a SstableInfo>,
    key_range: &R,
) -> Vec<&'a SstableInfo>
where
    R: RangeBounds<B>,
    B: AsRef<[u8]>,
{
    ssts.filter(|info| filter_single_sst(info, key_range))
        .collect()
}

pub fn can_concat(ssts: &[&SstableInfo]) -> bool {
    let len = ssts.len();
    for i in 0..len - 1 {
        if user_key(&ssts[i].get_key_range().as_ref().unwrap().right).cmp(user_key(
            &ssts[i + 1].get_key_range().as_ref().unwrap().left,
        )) != Ordering::Less
        {
            return false;
        }
    }
    true
}

/// Search the SST containing the specified key within a level, using binary search.
pub(crate) fn search_sst_idx<B>(ssts: &[&SstableInfo], key: &B) -> usize
where
    B: AsRef<[u8]> + Send + ?Sized,
{
    ssts.partition_point(|table| {
        let ord = user_key(&table.key_range.as_ref().unwrap().left).cmp(key.as_ref());
        ord == Ordering::Less || ord == Ordering::Equal
    })
    .saturating_sub(1) // considering the boundary of 0
}

struct MemoryLimiterInner {
    total_size: AtomicU64,
    notify: Notify,
    check_count: AtomicUsize,
    quota: u64,
}

impl MemoryLimiterInner {
    pub fn may_notify(&self, quota: u64) {
        while self.check_count.load(AtomicOrdering::SeqCst) > 0 {}
        let total_size = self.total_size.fetch_sub(quota, AtomicOrdering::Release);
        if total_size - quota < self.quota {
            self.notify.notify_waiters();
        }
    }
}

pub struct MemoryLimiter {
    inner: Arc<MemoryLimiterInner>,
}

pub struct MemoryTracker {
    limiter: Arc<MemoryLimiterInner>,
    quota: u64,
}

use std::sync::atomic::Ordering as AtomicOrdering;

impl MemoryLimiter {
    pub fn new(quota: u64) -> Self {
        Self {
            inner: Arc::new(MemoryLimiterInner {
                total_size: AtomicU64::new(0),
                notify: Notify::new(),
                check_count: AtomicUsize::new(0),
                quota,
            }),
        }
    }

    pub async fn require_memory(&self, quota: u64) -> Option<MemoryTracker> {
        if quota > self.inner.quota {
            return None;
        }
        let current_quota = self.inner.total_size.load(AtomicOrdering::Acquire);
        if current_quota < self.inner.quota {
            self.inner
                .total_size
                .fetch_add(quota, AtomicOrdering::Acquire);
            return Some(MemoryTracker {
                limiter: self.inner.clone(),
                quota,
            });
        }
        self.inner.check_count.fetch_add(1, AtomicOrdering::SeqCst);
        let mut current_quota = self.inner.total_size.load(AtomicOrdering::Acquire);
        while current_quota > self.inner.quota {
            let notified = self.inner.notify.notified();
            self.inner.check_count.fetch_sub(1, AtomicOrdering::SeqCst);
            notified.await;
            self.inner.check_count.fetch_add(1, AtomicOrdering::SeqCst);
            current_quota = self.inner.total_size.load(AtomicOrdering::Acquire) + quota;
        }
        self.inner
            .total_size
            .fetch_add(quota, AtomicOrdering::SeqCst);
        self.inner.check_count.fetch_sub(1, AtomicOrdering::SeqCst);
        Some(MemoryTracker {
            limiter: self.inner.clone(),
            quota,
        })
    }

    pub fn get_memory_usage(&self) -> u64 {
        self.inner.total_size.load(AtomicOrdering::Acquire)
    }
}

impl Drop for MemoryTracker {
    fn drop(&mut self) {
        self.limiter.may_notify(self.quota);
    }
}
