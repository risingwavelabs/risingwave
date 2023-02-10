use anyhow::Result;
use risingwave_pb::meta::TelemetryInfoResponse;
use tokio::sync::oneshot::Sender;
use tokio::task::JoinHandle;
use tokio::time::{interval, Duration};
use uuid::Uuid;

use super::{post_telemetry_report, TelemetryReport};

/// Url of telemetry backend
const TELEMETRY_REPORT_URL: &str = "http://localhost:8000/report";

/// Telemetry reporting interval in seconds, 24h
const TELEMETRY_REPORT_INTERVAL: u64 = 24 * 60 * 60;

#[async_trait::async_trait]
pub trait TelemetryInfoFetcher {
    async fn fetch_telemetry_info(&self) -> Result<TelemetryInfoResponse>;
}

pub fn start_telemetry_reporting<F, T, I>(
    info_fetcher: I,
    create_report: F,
) -> (JoinHandle<()>, Sender<()>)
where
    // tracking_id, session_id, up_time
    F: FnOnce(String, String, u64) -> T,
    T: TelemetryReport,
    I: TelemetryInfoFetcher,
    F: Send + Copy + 'static,
    T: Send + 'static,
    I: Send + Sync + 'static,
{
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel();
    let join_handle = tokio::spawn(async move {
        let fetcher = info_fetcher;
        let begin_time = std::time::Instant::now();
        let session_id = Uuid::new_v4().to_string();
        let mut interval = interval(Duration::from_secs(TELEMETRY_REPORT_INTERVAL));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                _ = interval.tick() => {},
                _ = &mut shutdown_rx => {
                    tracing::info!("Telemetry exit");
                    return;
                }
            }
            // fetch telemetry tracking_id and configs from the meta node
            let info = fetcher.fetch_telemetry_info().await;
            let (tracking_id, telemetry_enabled) = match info {
                Ok(resp) => (resp.tracking_id, resp.telemetry_enabled),
                Err(err) => {
                    tracing::error!("Telemetry failed to get tracking_id, err {}", err);
                    continue;
                }
            };

            // wait for the next interval, do not exit current thread
            if !telemetry_enabled {
                tracing::info!("Telemetry is not enabled");
                continue;
            }

            let report = create_report(
                tracking_id.clone(),
                session_id.clone(),
                begin_time.elapsed().as_secs(),
            );

            let report_json = match report.to_json() {
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
