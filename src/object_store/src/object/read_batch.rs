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

use std::ops::Range;
use std::sync::{Arc, Mutex};

use tokio::sync::{Notify, oneshot};
use tokio::time::Instant;

use super::read_scheduler::{PreparedRead, ReadPreparation, ReadResult, ReadScheduler};

/// A bounded registration scope. It coordinates cold requests, never block data.
/// Seal after registering the demand and neighbors. Dropping the owner releases
/// pending work without cancelling producers that may have other cache waiters.
pub struct ReadBatch {
    inner: Arc<BatchInner>,
}

pub struct ReadBatchRegistration {
    inner: Arc<BatchInner>,
    index: usize,
    role: Option<oneshot::Sender<bool>>,
}

/// Owned by the cache's external-fetch future, including before its first poll.
pub struct ReadBatchMember {
    inner: Arc<BatchInner>,
    index: usize,
    role: Option<oneshot::Receiver<bool>>,
}

struct BatchInner {
    scheduler: Arc<ReadScheduler>,
    changed: Notify,
    state: Mutex<BatchState>,
}

struct BatchState {
    capacity: usize,
    sealed: bool,
    closed: Option<&'static str>,
    resolved: Vec<bool>,
    remaining: usize,
    requests: Vec<PreparedRead>,
    // The first staged read transfers the lease to the driver.
    lease: Option<ReadPreparation>,
}

impl ReadBatch {
    pub(super) fn new(scheduler: Arc<ReadScheduler>, count: usize) -> Option<Self> {
        let lease = scheduler.reserve_preparation(count)?;
        Some(Self {
            inner: Arc::new(BatchInner {
                scheduler,
                changed: Notify::new(),
                state: Mutex::new(BatchState {
                    capacity: count,
                    sealed: false,
                    closed: None,
                    resolved: Vec::with_capacity(count),
                    remaining: 0,
                    requests: Vec::new(),
                    lease: Some(lease),
                }),
            }),
        })
    }

    pub fn register(&self) -> Option<(ReadBatchRegistration, ReadBatchMember)> {
        let mut state = self.inner.state.lock().unwrap();
        if state.closed.is_some() || state.sealed {
            return None;
        }
        // The reservation is fixed by the caller's already-bounded window.
        if state.resolved.len() == state.capacity {
            return None;
        }
        let index = state.resolved.len();
        state.resolved.push(false);
        state.remaining += 1;
        let (tx, rx) = oneshot::channel();
        Some((
            ReadBatchRegistration {
                inner: self.inner.clone(),
                index,
                role: Some(tx),
            },
            ReadBatchMember {
                inner: self.inner.clone(),
                index,
                role: Some(rx),
            },
        ))
    }

    pub fn seal(&self) {
        let mut state = self.inner.state.lock().unwrap();
        state.sealed = true;
        self.inner.finish_if_ready(&mut state);
    }
}

impl Drop for ReadBatch {
    fn drop(&mut self) {
        let mut state = self.inner.state.lock().unwrap();
        self.inner.close(&mut state, "window_cancelled");
    }
}

impl ReadBatchRegistration {
    /// Resolve the cache's authoritative registration role. A follower's retained
    /// fallback is still allowed to execute, but it must use the ordinary IO path.
    pub fn set_leader(mut self, leader: bool) {
        if !leader {
            self.inner.resolve(self.index);
        }
        let _ = self.role.take().unwrap().send(leader);
    }
}

impl Drop for ReadBatchRegistration {
    fn drop(&mut self) {
        if let Some(role) = self.role.take() {
            self.inner.resolve(self.index);
            let _ = role.send(false);
        }
    }
}

impl ReadBatchMember {
    pub(super) async fn stage(
        mut self,
        path: &str,
        range: Range<usize>,
        trace_id: Option<u64>,
    ) -> Option<oneshot::Receiver<ReadResult>> {
        // Foyer can poll a producer on another worker before registration returns.
        // Never infer the producer's role from which event arrives first.
        if !self.role.take().unwrap().await.unwrap_or(false) {
            return None;
        }
        let mut state = self.inner.state.lock().unwrap();
        if state.closed.is_some() {
            self.inner.scheduler.event("window_late");
            return None;
        }
        let (request, response) = PreparedRead::new(path, range, trace_id);
        state.requests.push(request);
        if !state.resolved[self.index] {
            state.resolved[self.index] = true;
            state.remaining -= 1;
        }
        if let Some(lease) = state.lease.take() {
            let inner = self.inner.clone();
            let start = Instant::now();
            // The bounded driver must progress even after a hot head returns and
            // the iterator is no longer polled. It owns the preparation lease.
            tokio::spawn(async move { inner.drive(start, lease).await });
        }
        self.inner.finish_if_ready(&mut state);
        Some(response)
    }
}

impl Drop for ReadBatchMember {
    fn drop(&mut self) {
        self.inner.resolve(self.index);
    }
}

impl BatchInner {
    fn resolve(&self, index: usize) {
        let mut state = self.state.lock().unwrap();
        if state.closed.is_none() && !state.resolved[index] {
            state.resolved[index] = true;
            state.remaining -= 1;
            self.finish_if_ready(&mut state);
        }
    }

    fn finish_if_ready(&self, state: &mut BatchState) {
        if state.sealed && state.remaining == 0 {
            self.close(state, "window_complete");
        }
    }

    fn close(&self, state: &mut BatchState, reason: &'static str) {
        if state.closed.is_none() {
            state.closed = Some(reason);
            // Late producer/registration guards need only observe closed, not the
            // old per-member state. Do not retain its allocation after closure.
            state.resolved = Vec::new();
            state.lease.take(); // All-hit windows never start a driver.
            self.scheduler.event(reason);
            self.changed.notify_one();
        }
    }

    async fn drive(self: Arc<Self>, start: Instant, lease: ReadPreparation) {
        let deadline = start + self.scheduler.preparation_timeout();
        loop {
            let notified = self.changed.notified();
            if self.state.lock().unwrap().closed.is_some() {
                break;
            }
            tokio::select! {
                _ = notified => {},
                _ = tokio::time::sleep_until(deadline) => {
                    self.close(&mut self.state.lock().unwrap(), "window_timeout");
                },
                _ = self.scheduler.closed() => {
                    self.close(&mut self.state.lock().unwrap(), "window_cancelled");
                },
            }
        }
        let (requests, reason) = {
            let mut state = self.state.lock().unwrap();
            (std::mem::take(&mut state.requests), state.closed.unwrap())
        };
        self.scheduler
            .submit_window(requests, lease, reason, start.elapsed())
            .await;
    }
}
