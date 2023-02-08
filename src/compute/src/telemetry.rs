// Copyright 2023 Singularity Data
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

use std::time::SystemTime;

use anyhow::Result;
use risingwave_common::telemetry::{
    post_telemetry_report, SystemData, TelemetryNodeType, TelemetryReportBase,
};
use risingwave_pb::meta::TelemetryInfoResponse;
use risingwave_rpc_client::MetaClient;
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot::Sender;
use tokio::task::JoinHandle;
use tokio::time::{interval, Duration};
use uuid::Uuid;

/// Url of telemetry backend
const TELEMETRY_REPORT_URL: &str = "http://localhost:8000/report";
/// Telemetry reporting interval in seconds, 24h
const TELEMETRY_REPORT_INTERVAL: u64 = 24 * 60 * 60;

pub const TELEMETRY_CF: &str = "cf/telemetry";
/// `telemetry` in bytes
pub const TELEMETRY_KEY: &[u8] = &[74, 65, 0x6c, 65, 0x6d, 65, 74, 72, 79];

#[derive(Debug, Serialize, Deserialize)]
struct TelemetryReport {
    #[serde(flatten)]
    base: TelemetryReportBase,
}

pub fn start_telemetry_reporting(meta_client: MetaClient) -> (JoinHandle<()>, Sender<()>) {
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

            // fetch telemetry tracking_id and configs from the meta node
            let (tracking_id, telemetry_enabled) =
                match get_tracking_info(meta_client.clone()).await {
                    Ok(resp) => (resp.tracking_id, resp.telemetry_enabled),
                    Err(err) => {
                        tracing::error!("Telemetry failed to get tracking_id, err {}", err);
                        continue;
                    }
                };

            // wait for the next interval, do not exit current thread
            if !telemetry_enabled {
                continue;
            }

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
                    node_type: TelemetryNodeType::Compute,
                },
            };

            let report_json = match serde_json::to_string(&report) {
                Ok(report_json) => report_json,
                Err(e) => {
                    tracing::error!("Telemetry failed to serialize report{}", e);
                    continue;
                }
            };

            match post_telemetry_report(TELEMETRY_REPORT_URL, report_json).await {
                Ok(_) => tracing::info!("Telemetry post success, id {}", tracking_id),
                Err(e) => tracing::error!("Telemetry post error, {}", e),
            }
        }
    });
    (join_handle, shutdown_tx)
}

async fn get_tracking_info(meta_client: MetaClient) -> Result<TelemetryInfoResponse> {
    let resp = meta_client.get_telemetry_info().await?;
    Ok(resp)
}
