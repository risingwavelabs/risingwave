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

use anyhow::anyhow;
use risingwave_common::telemetry::report::{TelemetryInfoFetcher, TelemetryReportCreator};
use risingwave_common::telemetry::{
    current_timestamp, SystemData, TelemetryNodeType, TelemetryReport, TelemetryReportBase,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::storage::MetaStore;

pub const TELEMETRY_CF: &str = "cf/telemetry";
/// `telemetry` in bytes
pub const TELEMETRY_KEY: &[u8] = &[74, 65, 0x6c, 65, 0x6d, 65, 74, 72, 79];

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct MetaTelemetryReport {
    #[serde(flatten)]
    base: TelemetryReportBase,
}

impl MetaTelemetryReport {
    pub(crate) fn new(tracking_id: String, session_id: String, up_time: u64) -> Self {
        Self {
            base: TelemetryReportBase {
                tracking_id,
                session_id,
                system_data: SystemData::new(),
                up_time,
                time_stamp: current_timestamp(),
                node_type: TelemetryNodeType::Meta,
            },
        }
    }
}

impl TelemetryReport for MetaTelemetryReport {
    fn to_json(&self) -> anyhow::Result<String> {
        let json = serde_json::to_string(self)?;
        Ok(json)
    }
}

pub(crate) struct MetaTelemetryInfoFetcher<S: MetaStore> {
    meta_store: Arc<S>,
}

impl<S: MetaStore> MetaTelemetryInfoFetcher<S> {
    pub(crate) fn new(meta_store: Arc<S>) -> Self {
        Self { meta_store }
    }
}

#[async_trait::async_trait]
impl<S: MetaStore> TelemetryInfoFetcher for MetaTelemetryInfoFetcher<S> {
    async fn fetch_telemetry_info(&self) -> anyhow::Result<String> {
        let tracking_id = get_or_create_tracking_id(self.meta_store.clone())
            .await
            .map(|id| id.to_string())?;

        Ok(tracking_id)
    }
}

/// fetch or create a `tracking_id` from etcd
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

#[derive(Copy, Clone)]
pub(crate) struct MetaReportCreator {}

impl MetaReportCreator {
    pub(crate) fn new() -> Self {
        Self {}
    }
}

impl TelemetryReportCreator for MetaReportCreator {
    fn create_report(
        &self,
        tracking_id: String,
        session_id: String,
        up_time: u64,
    ) -> anyhow::Result<MetaTelemetryReport> {
        Ok(MetaTelemetryReport::new(tracking_id, session_id, up_time))
    }

    fn report_type(&self) -> &str {
        "meta"
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::storage::MemStore;

    #[tokio::test]
    async fn test_get_or_create_tracking_id_existing_id() {
        let meta_store = Arc::new(MemStore::new());
        let uuid = Uuid::new_v4();
        meta_store
            .put_cf(
                TELEMETRY_CF,
                TELEMETRY_KEY.to_vec(),
                uuid.to_bytes_le().to_vec(),
            )
            .await
            .unwrap();
        let result = get_or_create_tracking_id(Arc::clone(&meta_store))
            .await
            .unwrap();
        assert_eq!(result, uuid);
    }

    #[tokio::test]
    async fn test_get_or_create_tracking_id_new_id() {
        let meta_store = Arc::new(MemStore::new());
        let result = get_or_create_tracking_id(Arc::clone(&meta_store))
            .await
            .unwrap();
        assert!(Uuid::from_slice_le(result.as_bytes()).is_ok());
    }
}
