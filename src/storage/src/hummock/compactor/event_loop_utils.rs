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

use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime};

use risingwave_pb::hummock::CompactTaskProgress;
use tokio::sync::oneshot::{Receiver, Sender};

use super::TaskProgress;

pub(super) const COMPACTION_HEARTBEAT_LOG_INTERVAL: Duration = Duration::from_secs(60);

static NEXT_SHUTDOWN_TOKEN: AtomicU64 = AtomicU64::new(1);

pub(super) struct LogThrottler<T: PartialEq> {
    last_logged_state: Option<T>,
    last_heartbeat: tokio::time::Instant,
    heartbeat_interval: Duration,
}

impl<T: PartialEq> LogThrottler<T> {
    pub(super) fn new(heartbeat_interval: Duration) -> Self {
        Self {
            last_logged_state: None,
            last_heartbeat: tokio::time::Instant::now(),
            heartbeat_interval,
        }
    }

    pub(super) fn should_log(&self, current_state: &T) -> bool {
        self.last_logged_state.as_ref() != Some(current_state)
            || self.last_heartbeat.elapsed() >= self.heartbeat_interval
    }

    pub(super) fn update(&mut self, current_state: T) {
        self.last_logged_state = Some(current_state);
        self.last_heartbeat = tokio::time::Instant::now();
    }
}

pub(super) fn unix_timestamp_millis() -> u64 {
    SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("Clock may have gone backwards")
        .as_millis() as u64
}

pub(super) fn pending_pull_task_count(
    max_task_parallelism: u32,
    max_pull_task_count: u32,
    running_parallelism: u32,
    pull_task_ack: bool,
) -> u32 {
    if !pull_task_ack {
        return 0;
    }

    (max_task_parallelism - running_parallelism).min(max_pull_task_count)
}

pub(super) enum StreamEvent<T> {
    Response(T),
    Reconnect,
}

pub(super) fn decode_stream_event<T>(
    event: Option<Result<T, tonic::Status>>,
    stream_name: &str,
) -> StreamEvent<T> {
    match event {
        Some(Ok(response)) => StreamEvent::Response(response),
        Some(Err(e)) => {
            tracing::warn!(stream_name, "Failed to consume stream. {}", e.message());
            StreamEvent::Reconnect
        }
        None => StreamEvent::Reconnect,
    }
}

pub(super) fn get_task_progress(
    task_progress: Arc<
        parking_lot::lock_api::Mutex<parking_lot::RawMutex, HashMap<u64, Arc<TaskProgress>>>,
    >,
) -> Vec<CompactTaskProgress> {
    let mut progress_list = Vec::new();
    for (&task_id, progress) in &*task_progress.lock() {
        progress_list.push(progress.snapshot(task_id));
    }
    progress_list
}

pub(super) struct TaskShutdownRegistration<K>
where
    K: Clone + Eq + Hash,
{
    pub(super) guard: TaskShutdownGuard<K>,
    pub(super) shutdown_rx: Receiver<()>,
    pub(super) replaced_existing: bool,
}

#[derive(Debug, PartialEq, Eq)]
pub(super) enum ShutdownCancelResult {
    Sent,
    AlreadyFinished,
    NotFound,
}

struct TaskShutdownEntry {
    token: u64,
    sender: Sender<()>,
}

pub(super) struct TaskShutdownRegistry<K>
where
    K: Clone + Eq + Hash,
{
    inner: Arc<std::sync::Mutex<HashMap<K, TaskShutdownEntry>>>,
}

impl<K> Default for TaskShutdownRegistry<K>
where
    K: Clone + Eq + Hash,
{
    fn default() -> Self {
        Self {
            inner: Arc::new(std::sync::Mutex::new(HashMap::new())),
        }
    }
}

impl<K> Clone for TaskShutdownRegistry<K>
where
    K: Clone + Eq + Hash,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<K> TaskShutdownRegistry<K>
where
    K: Clone + Eq + Hash,
{
    pub(super) fn register(&self, key: K) -> TaskShutdownRegistration<K> {
        let (sender, shutdown_rx) = tokio::sync::oneshot::channel();
        let token = NEXT_SHUTDOWN_TOKEN.fetch_add(1, Ordering::Relaxed);
        let replaced_existing = self
            .inner
            .lock()
            .unwrap()
            .insert(key.clone(), TaskShutdownEntry { token, sender })
            .is_some();

        TaskShutdownRegistration {
            guard: TaskShutdownGuard {
                registry: self.clone(),
                key,
                token,
            },
            shutdown_rx,
            replaced_existing,
        }
    }

    pub(super) fn cancel(&self, key: &K) -> ShutdownCancelResult {
        let Some(entry) = self.inner.lock().unwrap().remove(key) else {
            return ShutdownCancelResult::NotFound;
        };

        if entry.sender.send(()).is_err() {
            ShutdownCancelResult::AlreadyFinished
        } else {
            ShutdownCancelResult::Sent
        }
    }

    fn remove_if_current(&self, key: &K, token: u64) {
        let mut guard = self.inner.lock().unwrap();
        if guard.get(key).is_some_and(|entry| entry.token == token) {
            guard.remove(key);
        }
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.inner.lock().unwrap().len()
    }
}

pub(super) struct TaskShutdownGuard<K>
where
    K: Clone + Eq + Hash,
{
    registry: TaskShutdownRegistry<K>,
    key: K,
    token: u64,
}

impl<K> Drop for TaskShutdownGuard<K>
where
    K: Clone + Eq + Hash,
{
    fn drop(&mut self) {
        self.registry.remove_if_current(&self.key, self.token);
    }
}

#[cfg(test)]
mod tests {
    use tokio::sync::oneshot::error::TryRecvError;

    use super::*;

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct TestState {
        value: u32,
    }

    #[test]
    fn test_log_throttler_logs_on_state_change() {
        let mut throttler = LogThrottler::<TestState>::new(Duration::from_secs(60));
        let state1 = TestState { value: 1 };
        let state2 = TestState { value: 2 };

        assert!(throttler.should_log(&state1));
        throttler.update(state1.clone());
        assert!(!throttler.should_log(&state1));
        assert!(throttler.should_log(&state2));
    }

    #[tokio::test]
    async fn test_log_throttler_logs_on_heartbeat_interval() {
        let mut throttler = LogThrottler::<TestState>::new(Duration::from_millis(10));
        let state = TestState { value: 1 };

        throttler.update(state.clone());
        assert!(!throttler.should_log(&state));

        tokio::time::sleep(Duration::from_millis(15)).await;
        assert!(throttler.should_log(&state));
    }

    #[test]
    fn test_pending_pull_task_count_waits_for_ack() {
        assert_eq!(pending_pull_task_count(8, 4, 0, false), 0);
    }

    #[test]
    fn test_pending_pull_task_count_uses_available_parallelism() {
        assert_eq!(pending_pull_task_count(8, 4, 6, true), 2);
    }

    #[test]
    fn test_pending_pull_task_count_caps_by_max_pull_count() {
        assert_eq!(pending_pull_task_count(8, 4, 0, true), 4);
    }

    #[test]
    fn test_pending_pull_task_count_returns_zero_when_fully_occupied() {
        assert_eq!(pending_pull_task_count(8, 4, 8, true), 0);
    }

    #[test]
    fn test_decode_stream_event_returns_response() {
        match decode_stream_event(Some(Ok(233)), "test") {
            StreamEvent::Response(response) => assert_eq!(response, 233),
            StreamEvent::Reconnect => panic!("expected response"),
        }
    }

    #[test]
    fn test_decode_stream_event_reconnects_on_error() {
        match decode_stream_event::<u32>(Some(Err(tonic::Status::unavailable("closed"))), "test") {
            StreamEvent::Response(_) => panic!("expected reconnect"),
            StreamEvent::Reconnect => {}
        }
    }

    #[test]
    fn test_decode_stream_event_reconnects_on_end_of_stream() {
        match decode_stream_event::<u32>(None, "test") {
            StreamEvent::Response(_) => panic!("expected reconnect"),
            StreamEvent::Reconnect => {}
        }
    }

    #[test]
    fn test_shutdown_registry_cancel_removes_and_notifies() {
        let registry = TaskShutdownRegistry::<u64>::default();
        let mut registration = registry.register(233);
        assert_eq!(registry.len(), 1);

        assert_eq!(registry.cancel(&233), ShutdownCancelResult::Sent);
        assert_eq!(registry.len(), 0);
        assert!(registration.shutdown_rx.try_recv().is_ok());
    }

    #[test]
    fn test_shutdown_registry_cancel_unknown_returns_not_found() {
        let registry = TaskShutdownRegistry::<u64>::default();

        assert_eq!(registry.cancel(&233), ShutdownCancelResult::NotFound);
        assert_eq!(registry.len(), 0);
    }

    #[test]
    fn test_shutdown_registry_guard_drop_removes_registration() {
        let registry = TaskShutdownRegistry::<u64>::default();
        let mut registration = registry.register(233);
        assert_eq!(registry.len(), 1);

        drop(registration.guard);

        assert_eq!(registry.len(), 0);
        assert_eq!(registry.cancel(&233), ShutdownCancelResult::NotFound);
        assert!(matches!(
            registration.shutdown_rx.try_recv(),
            Err(TryRecvError::Closed)
        ));
    }

    #[test]
    fn test_shutdown_registry_cancel_after_receiver_drop_returns_already_finished() {
        let registry = TaskShutdownRegistry::<u64>::default();
        let registration = registry.register(233);
        assert_eq!(registry.len(), 1);

        drop(registration.shutdown_rx);

        assert_eq!(registry.cancel(&233), ShutdownCancelResult::AlreadyFinished);
        assert_eq!(registry.len(), 0);
    }

    #[test]
    fn test_shutdown_registry_replacing_registration_closes_old_receiver() {
        let registry = TaskShutdownRegistry::<u64>::default();
        let mut old_registration = registry.register(233);
        let new_registration = registry.register(233);
        assert!(new_registration.replaced_existing);
        assert_eq!(registry.len(), 1);

        assert!(matches!(
            old_registration.shutdown_rx.try_recv(),
            Err(TryRecvError::Closed)
        ));
    }

    #[test]
    fn test_shutdown_registry_stale_guard_does_not_remove_new_registration() {
        let registry = TaskShutdownRegistry::<u64>::default();
        let old_registration = registry.register(233);
        let mut new_registration = registry.register(233);
        assert!(new_registration.replaced_existing);
        assert_eq!(registry.len(), 1);

        drop(old_registration.guard);
        assert_eq!(registry.len(), 1);

        assert_eq!(registry.cancel(&233), ShutdownCancelResult::Sent);
        assert!(new_registration.shutdown_rx.try_recv().is_ok());
        assert_eq!(registry.len(), 0);
    }
}
