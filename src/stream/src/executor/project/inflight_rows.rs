// Copyright 2026 RisingWave Labs
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

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use tokio::sync::Notify;

/// A soft row budget: admit one whole chunk while usage is below the limit. Unlike acquiring
/// multiple semaphore permits, this lets a chunk larger than the limit make progress. Once usage
/// reaches the limit, further nonempty chunks wait for evaluation to finish and release rows.
pub(super) struct InflightRows {
    limit: usize,
    used: AtomicUsize,
    changed: Notify,
}

impl InflightRows {
    pub(super) fn new(limit: usize) -> Self {
        assert!(limit > 0);
        Self {
            limit,
            used: AtomicUsize::new(0),
            changed: Notify::new(),
        }
    }

    #[cfg(test)]
    pub(super) fn used_rows(&self) -> usize {
        self.used.load(Ordering::Acquire)
    }

    pub(super) async fn acquire(self: &Arc<Self>, rows: usize) -> InflightRowsPermit {
        loop {
            // Register before inspecting usage, so a concurrent release cannot lose a wakeup.
            let changed = self.changed.notified();
            if rows == 0
                || self
                    .used
                    .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                        (used < self.limit).then(|| {
                            used.checked_add(rows)
                                .expect("in-flight row count overflow")
                        })
                    })
                    .is_ok()
            {
                return InflightRowsPermit {
                    budget: self.clone(),
                    rows,
                };
            }
            changed.await;
        }
    }
}

pub(super) struct InflightRowsPermit {
    budget: Arc<InflightRows>,
    rows: usize,
}

impl Drop for InflightRowsPermit {
    fn drop(&mut self) {
        self.budget.used.fetch_sub(self.rows, Ordering::AcqRel);
        self.budget.changed.notify_waiters();
    }
}

#[cfg(test)]
mod tests {
    use futures::{FutureExt, poll};

    use super::*;

    #[tokio::test]
    async fn variable_chunks_overshoot_only_once() {
        let budget = Arc::new(InflightRows::new(5));
        let first = budget.acquire(3).await;
        let second = budget.acquire(4).await;
        assert_eq!(budget.used.load(Ordering::Acquire), 7);
        let third = budget.acquire(8);
        tokio::pin!(third);
        assert!(poll!(&mut third).is_pending());
        // Empty chunks never consume budget, even while it is exceeded.
        drop(budget.acquire(0).await);
        assert_eq!(budget.used.load(Ordering::Acquire), 7);
        drop(first);
        let third = third.await;
        assert_eq!(budget.used.load(Ordering::Acquire), 12);
        assert!(budget.acquire(1).now_or_never().is_none());
        drop(second);
        assert!(budget.acquire(1).now_or_never().is_none());
        drop(third);
        assert_eq!(budget.used.load(Ordering::Acquire), 0);
        drop(budget.acquire(100).await);
        assert_eq!(budget.used.load(Ordering::Acquire), 0);
    }

    #[tokio::test]
    async fn release_on_error_and_cancellation() {
        let budget = Arc::new(InflightRows::new(2));
        let failed: Result<(), ()> = async {
            let _permit = budget.acquire(3).await;
            Err(())
        }
        .await;
        assert!(failed.is_err());
        assert_eq!(budget.used.load(Ordering::Acquire), 0);

        let running = async {
            let _permit = budget.acquire(4).await;
            std::future::pending::<()>().await;
        };
        let mut running = Box::pin(running);
        assert!(poll!(&mut running).is_pending());
        assert_eq!(budget.used.load(Ordering::Acquire), 4);
        // Cancelling a waiter must not return another chunk's permits.
        assert!(budget.acquire(1).now_or_never().is_none());
        assert_eq!(budget.used.load(Ordering::Acquire), 4);
        drop(running);
        assert_eq!(budget.used.load(Ordering::Acquire), 0);
        drop(budget.acquire(2).await);
    }
}
