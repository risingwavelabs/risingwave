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
use std::sync::atomic::AtomicU64;
use std::sync::Arc;

use risingwave_hummock_sdk::key::user_key;
use risingwave_pb::hummock::{HummockVersion, Level, SstableInfo, WriteLimiterThreshold};
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
    quota: u64,
}

impl MemoryLimiterInner {
    pub fn release_quota(&self, quota: u64) {
        self.total_size.fetch_sub(quota, AtomicOrdering::Release);
        self.notify.notify_waiters();
    }

    pub fn try_require_memory(&self, quota: u64) -> bool {
        let mut current_quota = self.total_size.load(AtomicOrdering::Acquire);
        while current_quota + quota <= self.quota {
            match self.total_size.compare_exchange(
                current_quota,
                current_quota + quota,
                AtomicOrdering::SeqCst,
                AtomicOrdering::SeqCst,
            ) {
                Ok(_) => {
                    return true;
                }
                Err(old_quota) => {
                    current_quota = old_quota;
                }
            }
        }
        false
    }

    pub async fn require_memory(&self, quota: u64) {
        let current_quota = self.total_size.load(AtomicOrdering::Acquire);
        if current_quota + quota <= self.quota
            && self
                .total_size
                .compare_exchange(
                    current_quota,
                    current_quota + quota,
                    AtomicOrdering::SeqCst,
                    AtomicOrdering::SeqCst,
                )
                .is_ok()
        {
            // fast path.
            return;
        }
        loop {
            let notified = self.notify.notified();
            let current_quota = self.total_size.load(AtomicOrdering::Acquire);
            if current_quota + quota <= self.quota {
                match self.total_size.compare_exchange(
                    current_quota,
                    current_quota + quota,
                    AtomicOrdering::SeqCst,
                    AtomicOrdering::SeqCst,
                ) {
                    Ok(_) => break,
                    Err(old_quota) => {
                        // The quota is enough but just changed by other threads. So just try to
                        // update again without waiting notify.
                        if old_quota + quota <= self.quota {
                            continue;
                        }
                    }
                }
            }
            notified.await;
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
use std::time::Duration;

use crate::store::WriteDelay;

impl MemoryLimiter {
    pub fn unlimit() -> Arc<Self> {
        Arc::new(Self {
            inner: Arc::new(MemoryLimiterInner {
                total_size: AtomicU64::new(0),
                notify: Notify::new(),
                quota: u64::MAX - 1,
            }),
        })
    }

    pub fn new(quota: u64) -> Self {
        Self {
            inner: Arc::new(MemoryLimiterInner {
                total_size: AtomicU64::new(0),
                notify: Notify::new(),
                quota,
            }),
        }
    }

    pub fn can_require_memory(&self, quota: u64) -> bool {
        if quota > self.inner.quota {
            return false;
        }
        self.inner.total_size.load(AtomicOrdering::Acquire) + quota < self.inner.quota
    }

    pub async fn require_memory(&self, quota: u64) -> Option<MemoryTracker> {
        if quota > self.inner.quota {
            return None;
        }
        self.inner.require_memory(quota).await;
        Some(MemoryTracker {
            limiter: self.inner.clone(),
            quota,
        })
    }

    pub fn get_memory_usage(&self) -> u64 {
        self.inner.total_size.load(AtomicOrdering::Acquire)
    }
}

impl MemoryTracker {
    pub fn try_increase_memory(&mut self, target: u64) -> bool {
        if self.quota >= target {
            return true;
        }
        if self.limiter.try_require_memory(target - self.quota) {
            self.quota = target;
            true
        } else {
            false
        }
    }
}

impl Drop for MemoryTracker {
    fn drop(&mut self) {
        self.limiter.release_quota(self.quota);
    }
}

/// Check whether the items in `sub_iter` is a subset of the items in `full_iter`, and meanwhile
/// preserve the order.
pub fn check_subset_preserve_order<T: Eq>(
    sub_iter: impl Iterator<Item = T>,
    mut full_iter: impl Iterator<Item = T>,
) -> bool {
    for sub_iter_item in sub_iter {
        let mut found = false;
        for full_iter_item in full_iter.by_ref() {
            if sub_iter_item == full_iter_item {
                found = true;
                break;
            }
        }
        if !found {
            return false;
        }
    }
    true
}

/// Tells how long a write should be delayed before performed.
#[derive(Default)]
pub struct WriteLimiter {
    threshold: WriteLimiterThreshold,
    // Wakes stalled caller immediately.
    breaker_receivers: Vec<tokio::sync::oneshot::Sender<()>>,
    // Inputs of write delay calculation.
    sub_level_number: u64,
}

impl WriteLimiter {
    pub fn new() -> Self {
        Self {
            threshold: WriteLimiterThreshold {
                max_sub_level_number: u64::MAX,
                max_delay_sec: 0,
                per_file_delay_sec: 0.0,
            },
            breaker_receivers: vec![],
            sub_level_number: 0,
        }
    }

    pub fn get_write_delay(&mut self) -> Option<WriteDelay> {
        let exceeded = self
            .sub_level_number
            .saturating_sub(self.threshold.max_sub_level_number);
        let duration = std::cmp::min(
            self.threshold.max_delay_sec,
            (self.threshold.per_file_delay_sec * exceeded as f32) as u64,
        );
        if duration == 0 {
            return None;
        }
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.breaker_receivers.push(tx);
        Some(WriteDelay {
            duration: Duration::from_secs(duration),
            breaker: rx,
        })
    }

    pub fn set_threshold(&mut self, threshold: WriteLimiterThreshold) {
        self.threshold = threshold;
        for breaker in self.breaker_receivers.drain(..) {
            let _ = breaker.send(());
        }
    }

    pub fn set_stats(&mut self, version: &HummockVersion) {
        let mut sub_level_number = 0;
        for group in version.levels.values() {
            if let Some(l0) = group.l0.as_ref() {
                sub_level_number += l0.sub_levels.len();
            }
        }
        self.sub_level_number = sub_level_number as u64;
    }
}

#[cfg(test)]
mod tests {
    use std::iter;
    use std::time::Duration;

    use itertools::Itertools;
    use risingwave_pb::hummock::hummock_version::Levels;
    use risingwave_pb::hummock::{HummockVersion, Level, OverlappingLevel, WriteLimiterThreshold};

    use crate::hummock::utils::WriteLimiter;

    #[test]
    fn test_write_limiter() {
        let mut write_limiter = WriteLimiter::new();
        assert!(write_limiter.get_write_delay().is_none());

        write_limiter.set_threshold(WriteLimiterThreshold {
            max_sub_level_number: 101,
            max_delay_sec: 60,
            per_file_delay_sec: 1.0,
        });
        let mut version = HummockVersion::default();
        for i in 0..2 {
            version.levels.insert(
                i,
                Levels {
                    levels: vec![],
                    l0: Some(OverlappingLevel {
                        sub_levels: iter::once(Level::default()).cycle().take(50).collect_vec(),
                        total_file_size: 0,
                    }),
                },
            );
        }
        write_limiter.set_stats(&version);
        assert!(write_limiter.get_write_delay().is_none());

        write_limiter.set_threshold(WriteLimiterThreshold {
            max_sub_level_number: 99,
            max_delay_sec: 60,
            per_file_delay_sec: 1.0,
        });
        assert_eq!(
            write_limiter.get_write_delay().unwrap().duration,
            Duration::from_secs(1)
        );

        write_limiter.set_threshold(WriteLimiterThreshold {
            max_sub_level_number: 1,
            max_delay_sec: 60,
            per_file_delay_sec: 1.0,
        });
        assert_eq!(
            write_limiter.get_write_delay().unwrap().duration,
            Duration::from_secs(60)
        );
    }
}
