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

use crate::model::{MetadataModelError, MetadataModelResult};
use crate::storage::{MetaStore, Snapshot};

/// Column in meta store
pub const TELEMETRY_CF: &str = "cf/telemetry";
/// `telemetry` in bytes
pub const TELEMETRY_KEY: &[u8] = &[74, 65, 0x6c, 65, 0x6d, 65, 74, 72, 79];
#[derive(Clone, Debug)]
pub(crate) struct TrackingId(String);

impl TrackingId {
    pub(crate) fn new() -> Self {
        Self(Uuid::new_v4().to_string())
    }

    pub(crate) fn from_bytes(bytes: Vec<u8>) -> anyhow::Result<Self> {
        Ok(Self(String::from_utf8(bytes)?))
    }

    pub(crate) async fn from_meta_store(meta_store: &Arc<impl MetaStore>) -> anyhow::Result<Self> {
        match meta_store.get_cf(TELEMETRY_CF, TELEMETRY_KEY).await {
            Ok(bytes) => Self::from_bytes(bytes)
                .map_err(|e| anyhow::format_err!("failed to parse tracking_id {}", e)),
            Err(e) => Err(anyhow::format_err!("tracking_id not exist, {}", e)),
        }
    }

    pub(crate) async fn from_snapshot<S: MetaStore>(s: &S::Snapshot) -> MetadataModelResult<Self> {
        let bytes = s.get_cf(TELEMETRY_CF, TELEMETRY_KEY).await?;
        Self::from_bytes(bytes).map_err(MetadataModelError::internal)
    }

    /// fetch or create a `tracking_id` from etcd
    pub(crate) async fn get_or_create_meta_store(
        meta_store: &Arc<impl MetaStore>,
    ) -> Result<Self, anyhow::Error> {
        match Self::from_meta_store(meta_store).await {
            Ok(id) => Ok(id),
            Err(_) => {
                let tracking_id = Self::new();
                tracking_id.clone().put_at_meta_store(meta_store).await?;
                Ok(tracking_id)
            }
        }
    }

    pub(crate) async fn put_at_meta_store(
        &self,
        meta_store: &Arc<impl MetaStore>,
    ) -> anyhow::Result<()> {
        // put new uuid in meta store
        match meta_store
            .put_cf(
                TELEMETRY_CF,
                TELEMETRY_KEY.to_vec(),
                self.0.clone().into_bytes(),
            )
            .await
        {
            Err(e) => Err(anyhow!("failed to create uuid, {}", e)),
            Ok(_) => Ok(()),
        }
    }
}

impl From<TrackingId> for String {
    fn from(value: TrackingId) -> Self {
        value.0
    }
}
impl From<String> for TrackingId {
    fn from(value: String) -> Self {
        Self(value)
    }
}

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
        let tracking_id = TrackingId::from_meta_store(&self.meta_store).await?;

        Ok(tracking_id.into())
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

    #[test]
    fn test_tracking_id_new() {
        let tracking_id = TrackingId::new();
        assert!(!tracking_id.0.is_empty());
    }

    #[test]
    fn test_tracking_id_from_bytes() {
        let tracking_id = TrackingId::new();
        let bytes = tracking_id.0.clone().into_bytes();
        let new_tracking_id = TrackingId::from_bytes(bytes).unwrap();
        assert_eq!(tracking_id.0, new_tracking_id.0);
    }

    #[tokio::test]
    async fn test_tracking_id_put_and_get_from_meta_store() {
        let meta_store = Arc::new(MemStore::new());
        let tracking_id = TrackingId::new();

        tracking_id.put_at_meta_store(&meta_store).await.unwrap();
        let fetched_tracking_id = TrackingId::from_meta_store(&meta_store).await.unwrap();
        assert_eq!(tracking_id.0, fetched_tracking_id.0);
    }

    #[tokio::test]
    async fn test_tracking_id_get_or_create_meta_store() {
        let meta_store = Arc::new(MemStore::new());

        let tracking_id = TrackingId::get_or_create_meta_store(&meta_store)
            .await
            .unwrap();
        assert!(!tracking_id.0.is_empty());

        let fetched_tracking_id = TrackingId::get_or_create_meta_store(&meta_store)
            .await
            .unwrap();
        assert_eq!(tracking_id.0, fetched_tracking_id.0);
    }
}
