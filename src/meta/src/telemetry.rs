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

use std::sync::Arc;
use std::time::SystemTime;

use anyhow::anyhow;
use risingwave_common::telemetry::{
    post_telemetry_report, SystemData, TelemetryNodeType, TelemetryReportBase,
    TELEMETRY_REPORT_INTERVAL, TELEMETRY_REPORT_URL,
};
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot::Sender;
use tokio::task::JoinHandle;
use tokio::time::{interval, Duration};
use uuid::Uuid;

use crate::storage::MetaStore;

pub const TELEMETRY_CF: &str = "cf/telemetry";
/// `telemetry` in bytes
pub const TELEMETRY_KEY: &[u8] = &[74, 65, 0x6c, 65, 0x6d, 65, 74, 72, 79];

#[derive(Debug, Serialize, Deserialize)]
struct TelemetryReport {
    #[serde(flatten)]
    base: TelemetryReportBase,
}

/// This function spawns a new tokio task to report telemetry.
/// It creates a channel for killing itself and a join handle to the spawned task.
/// It then creates an interval of `TELEMETRY_REPORT_INTERVAL` seconds and checks if telemetry is
/// enabled. If it is, it gets or creates a tracking ID from the meta store,
/// creates a `TelemetryReport` object with system data, uptime, timestamp, and tracking ID.
/// Finally, it posts the report to `TELEMETRY_REPORT_URL`.
/// If an error occurs at any point in the process, it logs an error message.
pub async fn start_meta_telemetry_reporting(
    meta_store: Arc<impl MetaStore>,
) -> (JoinHandle<()>, Sender<()>) {
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel();
    let join_handle = tokio::spawn(async move {
        let begin_time = std::time::Instant::now();
        let session_id = Uuid::new_v4();
        let mut interval = interval(Duration::from_secs(TELEMETRY_REPORT_INTERVAL));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                _ = interval.tick() => {},
                _ = &mut shutdown_rx => {
                    return;
                }
            }

            let tracking_id = match get_or_create_tracking_id(meta_store.clone()).await {
                Ok(tracking_id) => tracking_id,
                Err(e) => {
                    tracing::error!("Telemetry fetch tacking id error {}", e);
                    continue;
                }
            };

            let report = TelemetryReport {
                base: TelemetryReportBase {
                    tracking_id: tracking_id.to_string(),
                    session_id: session_id.to_string(),
                    system_data: SystemData::new(),
                    up_time: begin_time.elapsed().as_secs(),
                    time_stamp: SystemTime::now()
                        .duration_since(SystemTime::UNIX_EPOCH)
                        .expect("Clock might go backward")
                        .as_secs(),
                    node_type: TelemetryNodeType::Meta,
                },
            };

            let report_json = match serde_json::to_string(&report) {
                Ok(report_json) => report_json,
                Err(e) => {
                    tracing::error!("Telemetry failed to serialize report{}", e);
                    continue;
                }
            };
            let url = TELEMETRY_REPORT_URL.to_owned() + "/meta";
            match post_telemetry_report(&url, report_json).await {
                Ok(_) => tracing::info!("Telemetry post success, id {}", tracking_id),
                Err(e) => tracing::error!("Telemetry post error, {}", e),
            }
        }
    });
    (join_handle, shutdown_tx)
}

/// fetch `tracking_id` from etcd
async fn get_or_create_tracking_id(meta_store: Arc<impl MetaStore>) -> Result<Uuid, anyhow::Error> {
    match meta_store.get_cf(TELEMETRY_CF, TELEMETRY_KEY).await {
        Ok(id) => Uuid::from_slice_le(&id).map_err(|e| anyhow!("failed to parse uuid, {}", e)),
        Err(_) => {
            let uuid = Uuid::new_v4();
            // put new uuid in meta store
            match meta_store
                .put_cf(
                    TELEMETRY_CF,
                    TELEMETRY_KEY.to_vec(),
                    uuid.to_bytes_le().to_vec(),
                )
                .await
            {
                Err(e) => Err(anyhow!("failed to create uuid, {}", e)),
                Ok(_) => Ok(uuid),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::storage::MemStore;

    #[tokio::test]
    async fn test_get_or_create_tracking_id_existing_id() {
        let meta_store = Arc::new(MemStore::new());
        let uuid = Uuid::new_v4();
        meta_store
            .put_cf(
                TELEMETRY_CF,
                TELEMETRY_KEY.to_vec(),
                uuid.to_bytes_le().to_vec(),
            )
            .await
            .unwrap();
        let result = get_or_create_tracking_id(Arc::clone(&meta_store))
            .await
            .unwrap();
        assert_eq!(result, uuid);
    }

    #[tokio::test]
    async fn test_get_or_create_tracking_id_new_id() {
        let meta_store = Arc::new(MemStore::new());
        let result = get_or_create_tracking_id(Arc::clone(&meta_store))
            .await
            .unwrap();
        assert!(Uuid::from_slice_le(result.as_bytes()).is_ok());
    }
}
