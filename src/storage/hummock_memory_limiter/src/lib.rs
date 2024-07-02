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

#![feature(arbitrary_self_types)]
#![feature(let_chains)]

mod sync;
mod test;

use std::collections::VecDeque;
use std::fmt::{Debug, Formatter};

use tokio::sync::oneshot::{channel, Receiver, Sender};

use crate::sync::atomic::{AtomicBool, AtomicU64, Ordering as AtomicOrdering};
use crate::sync::{Arc, Mutex};

type RequestQueue = VecDeque<(Sender<MemoryTracker>, u64)>;
enum MemoryRequest {
    Ready(MemoryTracker),
    Pending(Receiver<MemoryTracker>),
}

struct MemoryLimiterInner {
    total_size: AtomicU64,
    controller: Mutex<RequestQueue>,
    has_waiter: AtomicBool,
    quota: u64,
}

impl MemoryLimiterInner {
    fn release_quota(&self, quota: u64) {
        self.total_size.fetch_sub(quota, AtomicOrdering::SeqCst);
    }

    fn add_memory(&self, quota: u64) {
        self.total_size.fetch_add(quota, AtomicOrdering::SeqCst);
    }

    fn may_notify_waiters(self: &Arc<Self>) {
        // check `has_waiter` to avoid access lock every times drop `MemoryTracker`.
        if !self.has_waiter.load(AtomicOrdering::Acquire) {
            return;
        }
        {
            let mut waiters = self.controller.lock();
            while let Some((_, quota)) = waiters.front() {
                if !self.try_require_memory(*quota) {
                    break;
                }
                let (tx, quota) = waiters.pop_front().unwrap();
                if let Err(tracker) = tx.send(MemoryTracker::new(self.clone(), quota)) {
                    tracker.release_quota_without_notify();
                }
            }

            if waiters.is_empty() {
                self.has_waiter.store(false, AtomicOrdering::Release);
            }
        }
    }

    fn try_require_memory(&self, quota: u64) -> bool {
        let mut current_quota = self.total_size.load(AtomicOrdering::Acquire);
        while self.permit_quota(current_quota, quota) {
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

    fn require_memory(self: &Arc<Self>, quota: u64) -> MemoryRequest {
        let mut waiters = self.controller.lock();
        let first_req = waiters.is_empty();
        if first_req {
            // When this request is the first waiter but the previous `MemoryTracker` is just release a large quota, it may skip notifying this waiter because it has checked `has_waiter` and found it was false. So we must set it one and retry `try_require_memory` again to avoid deadlock.
            self.has_waiter.store(true, AtomicOrdering::Release);
        }
        // We must require again with lock because some other MemoryTracker may drop just after this thread gets mutex but before it enters queue.
        if self.try_require_memory(quota) {
            if first_req {
                self.has_waiter.store(false, AtomicOrdering::Release);
            }
            return MemoryRequest::Ready(MemoryTracker::new(self.clone(), quota));
        }
        let (tx, rx) = channel();
        waiters.push_back((tx, quota));
        MemoryRequest::Pending(rx)
    }

    fn permit_quota(&self, current_quota: u64, _request_quota: u64) -> bool {
        current_quota <= self.quota
    }
}

pub struct MemoryLimiter {
    inner: Arc<MemoryLimiterInner>,
}

impl Debug for MemoryLimiter {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemoryLimiter")
            .field("quota", &self.inner.quota)
            .field("usage", &self.inner.total_size)
            .finish()
    }
}

pub struct MemoryTracker {
    limiter: Arc<MemoryLimiterInner>,
    quota: Option<u64>,
}
impl MemoryTracker {
    fn new(limiter: Arc<MemoryLimiterInner>, quota: u64) -> Self {
        Self {
            limiter,
            quota: Some(quota),
        }
    }
}

impl Debug for MemoryTracker {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemoryTracker")
            .field("quota", &self.quota)
            .finish()
    }
}

impl MemoryLimiter {
    pub fn unlimit() -> Arc<Self> {
        Arc::new(Self::new(u64::MAX))
    }

    pub fn new(quota: u64) -> Self {
        Self {
            inner: Arc::new(MemoryLimiterInner {
                total_size: AtomicU64::new(0),
                controller: Mutex::new(VecDeque::default()),
                has_waiter: AtomicBool::new(false),
                quota,
            }),
        }
    }

    pub fn try_require_memory(&self, quota: u64) -> Option<MemoryTracker> {
        if self.inner.try_require_memory(quota) {
            Some(MemoryTracker::new(self.inner.clone(), quota))
        } else {
            None
        }
    }

    pub fn get_memory_usage(&self) -> u64 {
        self.inner.total_size.load(AtomicOrdering::Acquire)
    }

    pub fn quota(&self) -> u64 {
        self.inner.quota
    }

    pub fn must_require_memory(&self, quota: u64) -> MemoryTracker {
        if !self.inner.try_require_memory(quota) {
            self.inner.add_memory(quota);
        }

        MemoryTracker::new(self.inner.clone(), quota)
    }
}

impl MemoryLimiter {
    pub async fn require_memory(&self, quota: u64) -> MemoryTracker {
        match self.inner.require_memory(quota) {
            MemoryRequest::Ready(tracker) => tracker,
            MemoryRequest::Pending(rx) => rx.await.unwrap(),
        }
    }
}

impl MemoryTracker {
    pub fn try_increase_memory(&mut self, target: u64) -> bool {
        let quota = self.quota.unwrap();
        if quota >= target {
            return true;
        }
        if self.limiter.try_require_memory(target - quota) {
            self.quota = Some(target);
            true
        } else {
            false
        }
    }

    fn release_quota_without_notify(mut self) {
        let quota = self.quota.take().expect("should exist");
        self.limiter.release_quota(quota);
    }
}

// We must notify waiters outside `MemoryTracker` to avoid dead-lock and loop-owner.
impl Drop for MemoryTracker {
    fn drop(&mut self) {
        if let Some(quota) = self.quota.take() {
            self.limiter.release_quota(quota);
            self.limiter.may_notify_waiters();
        }
    }
}
