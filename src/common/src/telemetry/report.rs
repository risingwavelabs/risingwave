use anyhow::Result;
use risingwave_pb::meta::TelemetryInfoResponse;
use tokio::sync::oneshot::Sender;
use tokio::task::JoinHandle;
use tokio::time::{interval, Duration};
use uuid::Uuid;

use super::{
    post_telemetry_report, TelemetryReport, TELEMETRY_REPORT_INTERVAL, TELEMETRY_REPORT_URL,
};

#[async_trait::async_trait]
pub trait TelemetryInfoFetcher {
    async fn fetch_telemetry_info(&self) -> Result<TelemetryInfoResponse>;
}

pub trait TelemetryReportCreator {
    // inject dependencies to impl structs if more metrics needed
    fn create_report(
        &self,
        tracking_id: String,
        session_id: String,
        up_time: u64,
    ) -> Result<impl TelemetryReport>;

    fn report_type(&self) -> &str;
}

pub fn start_telemetry_reporting<F, I>(
    info_fetcher: I,
    report_creator: F,
) -> (JoinHandle<()>, Sender<()>)
where
    F: TelemetryReportCreator,
    I: TelemetryInfoFetcher,
    F: Send + Copy + 'static,
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
            let (tracking_id, telemetry_enabled, should_kill_telemetry) =
                match fetcher.fetch_telemetry_info().await {
                    Ok(resp) => (
                        resp.tracking_id,
                        resp.telemetry_enabled,
                        resp.should_kill_telemetry,
                    ),
                    Err(err) => {
                        tracing::error!("Telemetry failed to get tracking_id, err {}", err);
                        continue;
                    }
                };

            if should_kill_telemetry {
                tracing::info!("Telemetry is shutdown due to meta resp");
                return;
            }

            // wait for the next interval, do not exit current thread
            if !telemetry_enabled {
                tracing::info!("Telemetry is not enabled");
                continue;
            }

            // create a report and serialize to json
            let report_json = match report_creator
                .create_report(
                    tracking_id.clone(),
                    session_id.clone(),
                    begin_time.elapsed().as_secs(),
                )
                .map(|r| r.to_json())
            {
                Ok(Ok(report_json)) => report_json,
                Ok(Err(e)) => {
                    tracing::error!("Telemetry failed to serialize report to json, {}", e);
                    continue;
                }
                Err(e) => {
                    tracing::error!("Telemetry failed to create report {}", e);
                    continue;
                }
            };

            let url =
                (TELEMETRY_REPORT_URL.to_owned() + "/" + report_creator.report_type()).to_owned();

            match post_telemetry_report(&url, report_json).await {
                Ok(_) => tracing::info!("Telemetry post success, id {}", tracking_id),
                Err(e) => tracing::error!("Telemetry post error, {}", e),
            }
        }
    });
    (join_handle, shutdown_tx)
}
