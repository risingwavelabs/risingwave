// Copyright 2025 RisingWave Labs
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

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::SystemTime;

use parking_lot::RwLock;
use risingwave_pb::meta::EventLog as PbEventLog;
use risingwave_pb::meta::event_log::{Event as PbEvent, Event};
use tokio::task::JoinHandle;

pub type EventLogManagerRef = Arc<EventLogManger>;
type EventLogSender = tokio::sync::mpsc::Sender<EventLog>;
type ShutdownSender = tokio::sync::oneshot::Sender<()>;

/// Channel determines expiration strategy.
///
/// Currently all channels apply the same one strategy: keep latest N events.
///
/// Currently each event type has its own channel.
type ChannelId = u32;
type Channel = VecDeque<EventLog>;
type EventStoreRef = Arc<RwLock<HashMap<ChannelId, Channel>>>;

/// Spawns a task that's responsible for event log insertion and expiration.
pub fn start_event_log_manager(enabled: bool, event_log_channel_max_size: u32) -> EventLogManger {
    use futures::FutureExt;
    const BUFFER_SIZE: usize = 1024;
    let (event_tx, mut event_rx) = tokio::sync::mpsc::channel::<EventLog>(BUFFER_SIZE);
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let shutdown_rx_shared = shutdown_rx.shared();
    let event_logs: EventStoreRef = Arc::new(Default::default());
    let event_logs_shared = event_logs.clone();
    let worker_loop = async move {
        if !enabled {
            return;
        }
        loop {
            futures::select_biased! {
                _ = shutdown_rx_shared.clone().fuse() => {
                    tracing::info!("event log worker is stopped");
                    return;
                },
                event_log = event_rx.recv().fuse() => {
                    let Some(event_log) = event_log else {
                        tracing::info!("event log worker is stopped");
                        return;
                    };
                    let mut write = event_logs_shared.write();
                    let channel_id: ChannelId = (&event_log).into();
                    let channel = write.entry(channel_id).or_default();
                    channel.push_back(event_log);
                    // Apply expiration strategies.
                    keep_latest_n(channel, event_log_channel_max_size as _);
                },
            }
        }
    };
    let worker_join_handle = tokio::spawn(worker_loop);
    EventLogManger::new(
        event_tx,
        (worker_join_handle, shutdown_tx),
        enabled,
        event_logs,
    )
}

struct EventLog {
    payload: PbEventLog,
}

pub struct EventLogManger {
    event_tx: EventLogSender,
    worker_join_handle: RwLock<Option<(JoinHandle<()>, ShutdownSender)>>,
    enabled: bool,
    event_logs: EventStoreRef,
}

impl EventLogManger {
    fn new(
        event_tx: EventLogSender,
        worker_join_handle: (JoinHandle<()>, ShutdownSender),
        enabled: bool,
        event_logs: EventStoreRef,
    ) -> Self {
        if !enabled {
            tracing::info!("event log is disabled");
        }
        Self {
            event_tx,
            worker_join_handle: RwLock::new(Some(worker_join_handle)),
            enabled,
            event_logs,
        }
    }

    #[cfg(any(test, feature = "test"))]
    pub fn for_test() -> Self {
        let (event_tx, _event_rx) = tokio::sync::mpsc::channel(1);
        Self {
            event_tx,
            worker_join_handle: Default::default(),
            enabled: false,
            event_logs: Arc::new(Default::default()),
        }
    }

    pub fn take_join_handle(&self) -> Option<(JoinHandle<()>, ShutdownSender)> {
        self.worker_join_handle.write().take()
    }

    pub fn add_event_logs(&self, events: Vec<PbEvent>) {
        if !self.enabled {
            return;
        }
        let processing_ts = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        for event in events {
            let event_log = EventLog {
                payload: PbEventLog {
                    unique_id: Some(uuid::Uuid::new_v4().to_string()),
                    timestamp: Some(processing_ts),
                    event: Some(event),
                },
            };
            // Intentionally drop event logs if any error of buffer is full.
            if self.event_tx.try_send(event_log).is_err() {
                tracing::warn!("some event logs have been dropped");
                break;
            }
        }
    }

    pub fn list_event_logs(&self) -> Vec<PbEventLog> {
        self.event_logs
            .read()
            .values()
            .flat_map(|v| v.iter().map(|e| e.payload.to_owned()))
            .collect()
    }
}

fn keep_latest_n(channel: &mut Channel, max_n: usize) {
    while channel.len() > max_n {
        channel.pop_front();
    }
}

// TODO: avoid manual implementation
impl From<&EventLog> for ChannelId {
    fn from(value: &EventLog) -> Self {
        match value.payload.event.as_ref().unwrap() {
            Event::CreateStreamJobFail(_) => 1,
            Event::DirtyStreamJobClear(_) => 2,
            Event::MetaNodeStart(_) => 3,
            Event::BarrierComplete(_) => 4,
            Event::InjectBarrierFail(_) => 5,
            Event::CollectBarrierFail(_) => 6,
            Event::WorkerNodePanic(_) => 7,
            Event::AutoSchemaChangeFail(_) => 8,
            Event::SinkFail(_) => 9,
            Event::Recovery(_) => 10,
        }
    }
}
