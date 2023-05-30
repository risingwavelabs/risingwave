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

use anyhow::Result;
use tokio::sync::oneshot::Sender;
use tokio::task::JoinHandle;
use tokio::time::{interval, Duration};
use uuid::Uuid;

use super::{
    post_telemetry_report, TelemetryReport, TELEMETRY_REPORT_INTERVAL, TELEMETRY_REPORT_URL,
};

#[async_trait::async_trait]
pub trait TelemetryInfoFetcher {
    async fn fetch_telemetry_info(&self) -> Result<Option<String>>;
}

#[async_trait::async_trait]
pub trait TelemetryReportCreator {
    // inject dependencies to impl structs if more metrics needed
    async fn create_report(
        &self,
        tracking_id: String,
        session_id: String,
        up_time: u64,
    ) -> Result<impl TelemetryReport>;

    fn report_type(&self) -> &str;
}

pub async fn start_telemetry_reporting<F, I>(
    info_fetcher: Arc<I>,
    report_creator: Arc<F>,
) -> (JoinHandle<()>, Sender<()>)
where
    F: TelemetryReportCreator + Send + Sync + 'static,
    I: TelemetryInfoFetcher + Send + Sync + 'static,
{
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel();

    let join_handle = tokio::spawn(async move {
        tracing::info!("start telemetry reporting");

        let begin_time = std::time::Instant::now();
        let session_id = Uuid::new_v4().to_string();
        let mut interval = interval(Duration::from_secs(TELEMETRY_REPORT_INTERVAL));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        // fetch telemetry tracking_id from the meta node only at the beginning
        // There is only one case tracking_id updated at the runtime ---- etcd data has been
        // cleaned. There is no way that etcd has been cleaned but nodes are still running
        let tracking_id = match info_fetcher.fetch_telemetry_info().await {
            Ok(Some(id)) => id,
            Ok(None) => {
                tracing::info!("Telemetry is disabled");
                return;
            }
            Err(err) => {
                tracing::error!("Telemetry failed to get tracking_id, err {}", err);
                return;
            }
        };

        loop {
            tokio::select! {
                _ = interval.tick() => {},
                _ = &mut shutdown_rx => {
                    tracing::info!("Telemetry exit");
                    return;
                }
            }

            // create a report and serialize to json
            let report_json = match report_creator
                .create_report(
                    tracking_id.clone(),
                    session_id.clone(),
                    begin_time.elapsed().as_secs(),
                )
                .await
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
