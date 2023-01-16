use std::sync::Arc;

use anyhow::anyhow;
use tokio::time::{interval, Duration};
use uuid::Uuid;

use super::SystemData;
use crate::storage::MetaStore;

const REPORT_URL: &str = "unreachable";
/// interval in seconds
const REPORT_INTERVAL: u64 = 24 * 60 * 60;
const TELEMETRY_CF: &str = "cf/telemetry";
const TELEMETRY_KEY: &[u8] = &[74, 65, 0x6c, 65, 0x6d, 65, 74, 72, 79];

struct TelemetryReport {
    /// tracking_id is persistent in etcd
    tracking_id: String,
    /// session_id is reset every time Meta node restarts
    session_id: String,
    system: SystemData,
}

/// spawn a new tokio task to report telemetry
pub fn start_telemetry_reporting(meta_store: Arc<impl MetaStore>) {
    tokio::spawn(async move {
        let session_id = Uuid::new_v4();
        let mut interval = interval(Duration::from_secs(REPORT_INTERVAL));

        loop {
            interval.tick().await;

            if !telemetry_enabled() {
                continue;
            }

            let tracking_id = match meta_store.get_cf(TELEMETRY_CF, TELEMETRY_KEY).await {
                Ok(id) => {
                    Uuid::from_slice_le(&id).map_err(|e| anyhow!("failed to parse uuid, {}", e))
                }
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
            };

            let _report = TelemetryReport {
                tracking_id: tracking_id.unwrap().to_string(),
                session_id: session_id.to_string(),
                system: SystemData::new(),
            };
        }
    });
}

fn telemetry_enabled() -> bool {
    true
}
