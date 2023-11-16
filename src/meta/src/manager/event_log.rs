// Copyright 2023 RisingWave Labs
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

use std::iter;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use etcd_client::TxnOp;
use itertools::Itertools;
use parking_lot::RwLock;
use risingwave_pb::meta::event_log::EventType;
use risingwave_pb::meta::EventLog;
use tokio::task::JoinHandle;

use crate::storage::etcd_retry_client::EtcdRetryClient;
use crate::storage::{EtcdMetaStore, MetaStore, MetaStoreRef};
use crate::MetaResult;

pub type EventLogSender = tokio::sync::mpsc::Sender<EventLog>;

const BUFFER_SIZE: usize = 1024;
type LeaseId = i64;
type LeaseInfo = (LeaseId, Instant);
const CF: &[u8] = "cf/event_logs".as_bytes();

fn as_etcd_client(meta_store: &MetaStoreRef) -> Option<&EtcdRetryClient> {
    meta_store
        .as_any()
        .downcast_ref::<EtcdMetaStore>()
        .map(|e| e.client())
}

/// Adds event log kvs into etcd.
async fn add(
    client: &EtcdRetryClient,
    kvs: impl IntoIterator<Item = (Vec<u8>, Vec<u8>)>,
    lease_id: LeaseId,
    max_size: u64,
) -> Result<(), etcd_client::Error> {
    let opt = etcd_client::PutOptions::new().with_lease(lease_id);
    let ops = kvs
        .into_iter()
        .map(|(k, v)| TxnOp::put(get_full_key(k, max_size), v, Some(opt.clone())))
        .collect_vec();
    let etcd_txn = etcd_client::Txn::new().and_then(ops);
    client.txn(etcd_txn).await?;
    Ok(())
}

/// Lists event log kvs from etcd.
async fn list(client: &EtcdRetryClient) -> Result<Vec<Vec<u8>>, etcd_client::Error> {
    let opt = etcd_client::GetOptions::new().with_prefix();
    client
        .get(get_key_prefix().into_iter().collect_vec(), Some(opt))
        .await
        .map(|v| v.kvs().iter().map(|kv| kv.value().to_vec()).collect())
}

fn get_key_prefix() -> impl IntoIterator<Item = u8> {
    CF.iter().cloned().chain(iter::once(b'/'))
}

fn get_full_key(k: impl IntoIterator<Item = u8>, max_payload_size: u64) -> Vec<u8> {
    get_key_prefix()
        .into_iter()
        .chain(k.into_iter().take(max_payload_size as usize))
        .collect()
}

pub fn start_event_log_manager(
    meta_store: MetaStoreRef,
    enabled: bool,
    lease_ttl_sec: u64,
    flush_interval_ms: u64,
    max_payload_size: u64,
) -> EventLogManger {
    use futures::FutureExt;
    use prost::Message;
    let (event_tx, mut event_rx) = tokio::sync::mpsc::channel::<EventLog>(BUFFER_SIZE);
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let meta_store_clone = meta_store.clone();
    let mut last_lease = None;
    let shutdown_rx_shared = shutdown_rx.shared();
    let worker_loop = async move {
        let mut flush_interval = tokio::time::interval(Duration::from_millis(flush_interval_ms));
        flush_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        flush_interval.reset();
        let client = match as_etcd_client(&meta_store_clone) {
            None => return,
            Some(c) => c,
        };
        let mut event_log_batch: Vec<EventLog> = vec![];
        loop {
            futures::select_biased! {
                _ = shutdown_rx_shared.clone().fuse() => {
                    tracing::info!("event log worker is stopped");
                    return;
                },
                _ = flush_interval.tick().fuse() => {
                    // Flush batched event logs to persistent storage.
                    if event_log_batch.is_empty() {
                        continue;
                    }
                    // Intentionally drop events on any error.
                    let batch = event_log_batch.drain(..);
                    last_lease = match may_get_new_lease(client, last_lease, lease_ttl_sec).await {
                        Ok(last_lease) => last_lease,
                        Err(e) => {
                            tracing::warn!("Some event logs are not persisted: {}", e);
                            continue;
                        }
                    };
                    if let Err(e) = add(
                        client,
                        batch.map(|e|(e.unique_id.to_owned().unwrap().as_bytes().into(),e.encode_to_vec())),
                        last_lease.as_ref().unwrap().0,
                        max_payload_size,
                    )
                    .await
                    {
                        tracing::warn!("Some event logs are not persisted: {}", e);
                        continue;
                    }
                }
                event_log = event_rx.recv().fuse() => {
                    // Batch event logs.
                    match event_log {
                        None => {
                            break;
                        }
                        Some(event_log) => {
                            event_log_batch.push(event_log);
                        }
                    };
                },
            }
        }
    };
    let worker_join_handle = tokio::spawn(worker_loop);
    EventLogManger::new(
        meta_store,
        event_tx,
        (worker_join_handle, shutdown_tx),
        enabled,
    )
}

/// Gets a lease to attach it to new keys. The function
/// - either returns the old lease,
/// - or grant a new lease, if the old lease is likely to expire within `lease_ttl_sec`.
///
/// The idea is to make a trade-off between total number of lease granted and timeliness of key expiration.
/// New lease's ttl is set to `lease_ttl_sec` + `lease_min_interval_sec`.
/// Whenever a lease's remaining ttl is less than `lease_ttl_sec`, it should not be used for new keys anymore.
async fn may_get_new_lease(
    etcd_client: &EtcdRetryClient,
    old_lease: Option<LeaseInfo>,
    lease_ttl_sec: u64,
) -> Result<Option<LeaseInfo>, etcd_client::Error> {
    // `lease_min_interval_sec` is to avoid granting a new lease for each put op.
    // When a lease has lived for over `lease_min_interval_sec`, a new lease should be granted to meet `lease_ttl_sec`.
    let lease_min_interval_sec = std::cmp::min(3600, lease_ttl_sec);
    let get_new = match &old_lease {
        None => true,
        Some(l) => l.1.elapsed().as_secs() >= lease_min_interval_sec,
    };
    if !get_new {
        return Ok(old_lease);
    }
    let new_lease_id = etcd_client
        .grant((lease_ttl_sec + lease_min_interval_sec) as i64, None)
        .await?
        .id();
    Ok(Some((new_lease_id, Instant::now())))
}

pub type EventLogMangerRef = Arc<EventLogManger>;
type ShutdownSender = tokio::sync::oneshot::Sender<()>;

pub struct EventLogManger {
    meta_store: MetaStoreRef,
    event_tx: EventLogSender,
    worker_join_handle: RwLock<Option<(JoinHandle<()>, ShutdownSender)>>,
    enabled: bool,
}

impl EventLogManger {
    pub fn new(
        meta_store: MetaStoreRef,
        event_tx: EventLogSender,
        worker_join_handle: (JoinHandle<()>, ShutdownSender),
        enabled: bool,
    ) -> Self {
        if !enabled {
            tracing::info!("event log is disabled");
        }
        Self {
            meta_store,
            event_tx,
            worker_join_handle: RwLock::new(Some(worker_join_handle)),
            enabled,
        }
    }

    #[cfg(any(test, feature = "test"))]
    pub fn for_test(meta_store: MetaStoreRef) -> Self {
        let (event_tx, _event_rx) = tokio::sync::mpsc::channel(BUFFER_SIZE);
        Self {
            meta_store,
            event_tx,
            worker_join_handle: Default::default(),
            enabled: false,
        }
    }

    pub fn take_join_handle(&self) -> Option<(JoinHandle<()>, ShutdownSender)> {
        self.worker_join_handle.write().take()
    }

    pub fn add_event_logs(&self, event_logs: Vec<EventLog>) -> MetaResult<()> {
        if !self.enabled {
            for event_log in event_logs {
                tracing::debug!("{}", display_event_log(&event_log));
            }
            return Ok(());
        }
        let mut all_succ = true;
        let processing_ts = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        for mut event_log in event_logs {
            event_log.unique_id = Some(uuid::Uuid::new_v4().to_string());
            event_log.timestamp = Some(processing_ts);
            // Intentionally drop event logs if any error of buffer is full.
            if let Err(e) = self.event_tx.try_send(event_log.clone()) {
                tracing::warn!(
                    "event log is not persisted: {}, error {}",
                    display_event_log(&event_log),
                    e
                );
                all_succ = false;
            }
        }
        if all_succ {
            Ok(())
        } else {
            Err(anyhow::anyhow!("some event logs failed to persist").into())
        }
    }

    pub async fn list_event_logs(&self) -> MetaResult<Vec<EventLog>> {
        use prost::Message;
        let event_logs = match as_etcd_client(&self.meta_store) {
            None => vec![],
            Some(c) => list(c)
                .await
                .map(|v| {
                    v.into_iter()
                        .map(|e| EventLog::decode(e.as_slice()).unwrap())
                        .collect()
                })
                .map_err(|e| anyhow::anyhow!("failed to list event logs: {}", e))?,
        };
        Ok(event_logs)
    }
}

fn display_event_log(event_log: &EventLog) -> String {
    format!(
        "id: {}, timestamp: {}, type: {}, info: {}",
        event_log.unique_id.to_owned().unwrap_or_default(),
        event_log.timestamp.to_owned().unwrap_or_default(),
        event_log.event_type().as_str_name(),
        event_log.info
    )
}

pub fn new_event_log(event_type: EventType, info: String) -> EventLog {
    EventLog {
        unique_id: None,
        timestamp: None,
        event_type: event_type as _,
        info,
    }
}
