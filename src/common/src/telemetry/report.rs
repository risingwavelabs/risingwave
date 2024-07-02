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

use prost::Message;
use risingwave_pb::telemetry::{
    EventMessage as PbEventMessage, PbTelemetryDatabaseComponents,
    TelemetryEventStage as PbTelemetryEventStage,
};
use tokio::sync::oneshot::Sender;
use tokio::task::JoinHandle;
use tokio::time::{interval, Duration};
use uuid::Uuid;

use super::{current_timestamp, Result, TELEMETRY_REPORT_INTERVAL, TELEMETRY_REPORT_URL};
use crate::telemetry::pb_compatible::TelemetryToProtobuf;
use crate::telemetry::post_telemetry_report_pb;

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
    set_tracking_id_and_session_id: Arc<dyn Fn(String, String) + Send + Sync>,
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
        set_tracking_id_and_session_id(tracking_id.clone(), session_id.clone());

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

pub fn report_event_common(
    get_tracking_id_and_session_id: Box<dyn Fn() -> (Option<String>, Option<String>) + Send + Sync>,
    event_stage: PbTelemetryEventStage,
    feature_name: String,
    catalog_id: i64,
    connector_name: Option<String>,
    components: Option<PbTelemetryDatabaseComponents>,
    attributes: Option<String>, // any json string
    node: String,
) {
    // if disabled the telemetry, tracking_id and session_id will be None, so the event will not be reported
    let event_tracking_id: String;
    let event_session_id: String;
    let (tracking_id, session_id) = get_tracking_id_and_session_id();
    if let Some(tracing_id) = tracking_id
        && let Some(session_id) = session_id
    {
        event_session_id = session_id;
        event_tracking_id = tracing_id;
    } else {
        tracing::info!(
            "got empty tracking_id or session_id, Telemetry is disabled or not initialized"
        );
        return;
    }
    let event = PbEventMessage {
        tracking_id: event_tracking_id,
        session_id: event_session_id,
        event_time_sec: current_timestamp(),
        event_stage: event_stage as i32,
        feature_name,
        connector_name,
        component: components.map(|c| c as i32),
        catalog_id,
        attributes,
        node,
    };
    let report_bytes = event.encode_to_vec();

    tokio::spawn(async move {
        const TELEMETRY_EVENT_REPORT_TYPE: &str = "event";
        let url = (TELEMETRY_REPORT_URL.to_owned() + "/" + TELEMETRY_EVENT_REPORT_TYPE).to_owned();

        post_telemetry_report_pb(&url, report_bytes)
            .await
            .unwrap_or_else(|e| tracing::warn!("{}", e))
    });
}
