use std::sync::Arc;

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
    system: SystemData,
}

struct TelemetryReporter {
    /// tracking_id is persistent in etcd
    tracking_id: Uuid,
    /// session_id is reset every time Meta node restarts
    session_id: Uuid,
}

impl TelemetryReporter {
    pub async fn new() -> Self {
        Self {
            tracking_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
        }
    }

    pub async fn run(meta_store: Arc<impl MetaStore>) {
        tokio::spawn(async move {
            let tracking_id = match meta_store.get_cf(TELEMETRY_CF, TELEMETRY_KEY).await {
                Ok(id) => Uuid::from_slice_le(&id),
                Err(_) => {
                    let uuid = Uuid::new_v4();
                    let res = meta_store
                        .put_cf(
                            TELEMETRY_CF,
                            TELEMETRY_KEY.to_vec(),
                            uuid.to_bytes_le().to_vec(),
                        )
                        .await;
                    Ok(uuid)
                }
            };

            let mut interval = interval(Duration::from_secs(REPORT_INTERVAL));

            loop {
                interval.tick().await;

                if !telemetry_enabled() {
                    continue;
                }

                let _report = TelemetryReport {
                    system: SystemData::new(),
                };
            }
        });
    }
}

fn telemetry_enabled() -> bool {
    true
}
