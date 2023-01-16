use std::sync::Arc;

use anyhow::anyhow;
use hyper::StatusCode;
use serde::{Deserialize, Serialize};
use tokio::time::{interval, Duration};
use uuid::Uuid;

use super::SystemData;
use crate::storage::MetaStore;
use crate::stream::SourceManager;

const TELEMETRY_REPORT_URL: &str = "unreachable";
/// interval in seconds
const TELEMETRY_REPORT_INTERVAL: u64 = 24 * 60 * 60;
const TELEMETRY_CF: &str = "cf/telemetry";
/// `telemetry` in bytes
const TELEMETRY_KEY: &[u8] = &[74, 65, 0x6c, 65, 0x6d, 65, 74, 72, 79];

#[derive(Debug, Serialize, Deserialize)]
struct TelemetryReport {
    /// tracking_id is persistent in etcd
    tracking_id: String,
    /// session_id is reset every time Meta node restarts
    session_id: String,
    system: SystemData,
    // number of sources created
    source_count: usize,
}

/// spawn a new tokio task to report telemetry
pub fn start_telemetry_reporting(
    meta_store: Arc<impl MetaStore>,
    source_manager: Arc<SourceManager<impl MetaStore>>,
) {
    tokio::spawn(async move {
        let session_id = Uuid::new_v4();
        let mut interval = interval(Duration::from_secs(TELEMETRY_REPORT_INTERVAL));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            interval.tick().await;

            if !telemetry_enabled() {
                continue;
            }

            if let Ok(tracking_id) = fetch_tracking_id(meta_store.clone()).await {
                let report = TelemetryReport {
                    tracking_id: tracking_id.to_string(),
                    session_id: session_id.to_string(),
                    system: SystemData::new(),
                    source_count: source_manager.source_count().await,
                };

                post_telemetry_report(TELEMETRY_REPORT_URL, &report)
                    .await
                    .unwrap();
            }
        }
    });
}

async fn post_telemetry_report(url: &str, report: &TelemetryReport) -> Result<(), anyhow::Error> {
    let http_client = hyper::Client::new();
    let report_json = serde_json::to_string(report)?;
    let req = hyper::Request::post(url)
        .header("Content-Type", "application/json")
        .body(hyper::Body::from(report_json))?;

    let res = http_client.request(req).await?;
    if res.status() == StatusCode::OK {
        Ok(())
    } else {
        Err(anyhow!("invalid telemetry resp status, {}", res.status()))
    }
}

async fn fetch_tracking_id(meta_store: Arc<impl MetaStore>) -> Result<Uuid, anyhow::Error> {
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

fn telemetry_enabled() -> bool {
    true
}

#[cfg(test)]
mod tests {
    use httpmock::Method::POST;
    use httpmock::MockServer;
    use uuid::Uuid;

    use super::*;
    use crate::telemetry::SystemData;

    #[tokio::test]
    async fn test_post_telemetry_report_success() {
        let mock_server = MockServer::start();
        let url = mock_server.url("/report");

        let report = TelemetryReport {
            tracking_id: Uuid::new_v4().to_string(),
            session_id: Uuid::new_v4().to_string(),
            system: SystemData::new(),
            source_count: 10,
        };
        let report_json = serde_json::to_string(&report).unwrap();
        let resp_mock = mock_server.mock(|when, then| {
            when.method(POST)
                .path("/report")
                .header("Content-Type", "application/json")
                .body(report_json);
            then.status(200);
        });
        post_telemetry_report(&url, &report).await.unwrap();
        resp_mock.assert();
    }
}
