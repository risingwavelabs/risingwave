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

use std::sync::Arc;
use std::time::SystemTime;

use anyhow::anyhow;
use hyper::StatusCode;
use risingwave_common::telemetry::SystemData;
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot::Sender;
use tokio::task::JoinHandle;
use tokio::time::{interval, Duration};
use uuid::Uuid;

use crate::storage::MetaStore;

/// Environment Variable that is default to be true
const TELEMETRY_ENV_ENABLE: &str = "ENABLE_TELEMETRY";
/// Url of telemetry backend
const TELEMETRY_REPORT_URL: &str = "unreachable";
/// Telemetry reporting interval in seconds, 24h
const TELEMETRY_REPORT_INTERVAL: u64 = 24 * 60 * 60;
pub const TELEMETRY_CF: &str = "cf/telemetry";
/// `telemetry` in bytes
pub const TELEMETRY_KEY: &[u8] = &[74, 65, 0x6c, 65, 0x6d, 65, 74, 72, 79];

#[derive(Debug, Serialize, Deserialize)]
struct TelemetryReport {
    /// tracking_id is persistent in etcd
    tracking_id: String,
    /// session_id is reset every time Meta node restarts
    session_id: String,
    system: SystemData,
    up_time: u64,
    time_stamp: u64,
}

/// spawn a new tokio task to report telemetry
pub async fn start_telemetry_reporting(
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

            if !telemetry_enabled() {
                tracing::info!("Telemetry not enabled");
                continue;
            }

            match get_or_create_tracking_id(meta_store.clone()).await {
                Ok(tracking_id) => {
                    let report = TelemetryReport {
                        tracking_id: tracking_id.to_string(),
                        session_id: session_id.to_string(),
                        system: SystemData::new(),
                        up_time: begin_time.elapsed().as_secs(),
                        time_stamp: SystemTime::now()
                            .duration_since(SystemTime::UNIX_EPOCH)
                            .expect("Clock might go backward")
                            .as_secs(),
                    };

                    if let Err(e) = post_telemetry_report(TELEMETRY_REPORT_URL, &report).await {
                        tracing::error!("Telemetry post error, {}", e);
                    }
                }
                Err(e) => {
                    tracing::error!("Telemetry fetch tacking id error {}", e);
                }
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

/// check whether telemetry is enabled
fn telemetry_enabled() -> bool {
    // default to be true
    std::env::var(TELEMETRY_ENV_ENABLE)
        .unwrap_or("true".to_string())
        .trim()
        .to_ascii_lowercase()
        .parse()
        .unwrap_or(true)
}

impl TelemetryReport {
    #[cfg(test)]
    fn for_test() -> Self {
        Self {
            tracking_id: Uuid::new_v4().to_string(),
            session_id: Uuid::new_v4().to_string(),
            system: SystemData::new(),
            up_time: 123123,
            time_stamp: 10,
        }
    }
}

#[cfg(test)]
mod tests {
    use httpmock::Method::POST;
    use httpmock::MockServer;

    use super::*;

    #[test]
    fn test_telemetry_enabled() {
        assert!(telemetry_enabled());
        std::env::set_var(TELEMETRY_ENV_ENABLE, "false");
        assert!(!telemetry_enabled());
        std::env::set_var(TELEMETRY_ENV_ENABLE, "wrong_str");
        assert!(telemetry_enabled());
        std::env::set_var(TELEMETRY_ENV_ENABLE, "False");
        assert!(!telemetry_enabled());
    }
}
