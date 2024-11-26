// Copyright 2024 RisingWave Labs
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

use risingwave_telemetry_event::get_telemetry_risingwave_cloud_uuid;
pub use risingwave_telemetry_event::{
    current_timestamp, post_telemetry_report_pb, TELEMETRY_REPORT_URL, TELEMETRY_TRACKING_ID,
};
use tokio::sync::oneshot::Sender;
use tokio::task::JoinHandle;
use tokio::time::{interval, Duration};
use uuid::Uuid;

use super::{Result, TELEMETRY_REPORT_INTERVAL};
use crate::telemetry::pb_compatible::TelemetryToProtobuf;

#[async_trait::async_trait]
pub trait TelemetryInfoFetcher {
    /// Fetches telemetry info from meta. Currently it's only `tracking_id` (`cluster_id`).
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
    ) -> Result<impl TelemetryToProtobuf>;

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
        // There is only one case tracking_id updated at the runtime ---- metastore data has been
        // cleaned. There is no way that metastore has been cleaned but nodes are still running
        let tracking_id = {
            if let Some(cloud_uuid) = get_telemetry_risingwave_cloud_uuid() {
                cloud_uuid
            } else {
                match info_fetcher.fetch_telemetry_info().await {
                    Ok(Some(id)) => id,
                    Ok(None) => {
                        tracing::info!("Telemetry is disabled");
                        return;
                    }
                    Err(err) => {
                        tracing::error!("Telemetry failed to get tracking_id, err {}", err);
                        return;
                    }
                }
            }
        };
        TELEMETRY_TRACKING_ID
            .set(tracking_id.clone())
            .unwrap_or_else(|_| {
                tracing::warn!(
                    "Telemetry failed to set tracking_id, event reporting will be disabled"
                )
            });

        loop {
            tokio::select! {
                _ = interval.tick() => {},
                _ = &mut shutdown_rx => {
                    tracing::info!("Telemetry exit");
                    return;
                }
            }

            // create a report and serialize to json
            let bin_report = match report_creator
                .create_report(
                    tracking_id.clone(),
                    session_id.clone(),
                    begin_time.elapsed().as_secs(),
                )
                .await
                .map(TelemetryToProtobuf::to_pb_bytes)
            {
                Ok(bin_report) => bin_report,
                Err(e) => {
                    tracing::error!("Telemetry failed to create report {}", e);
                    continue;
                }
            };

            let url =
                (TELEMETRY_REPORT_URL.to_owned() + "/" + report_creator.report_type()).to_owned();

            match post_telemetry_report_pb(&url, bin_report).await {
                Ok(_) => tracing::info!("Telemetry post success, id {}", tracking_id),
                Err(e) => tracing::error!("Telemetry post error, {}", e),
            }
        }
    });
    (join_handle, shutdown_tx)
}
