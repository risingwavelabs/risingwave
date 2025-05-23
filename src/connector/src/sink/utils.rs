// Copyright 2025 RisingWave Labs
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

use risingwave_common::array::StreamChunk;
use serde_json::Value;

use super::encoder::{JsonEncoder, RowEncoder};
use crate::sink::Result;

pub fn chunk_to_json(chunk: StreamChunk, encoder: &JsonEncoder) -> Result<Vec<String>> {
    let mut records: Vec<String> = Vec::with_capacity(chunk.capacity());
    for (_, row) in chunk.rows() {
        let record = Value::Object(encoder.encode(row)?);

        records.push(record.to_string());
    }

    Ok(records)
}

/// Dummy trait implementation for a sink when the feature is not enabled at compile time.
pub(crate) mod dummy {

    use std::collections::BTreeMap;
    use std::fmt::{Debug, Formatter};
    use std::marker::PhantomData;

    use anyhow::anyhow;
    use phf::{Set, phf_set};
    use risingwave_pb::connector_service::SinkMetadata;
    use sea_orm::DatabaseConnection;
    use tokio::sync::mpsc::UnboundedSender;

    use crate::connector_common::IcebergSinkCompactionUpdate;
    use crate::enforce_secret::EnforceSecret;
    use crate::error::ConnectorResult;
    use crate::sink::prelude::*;
    use crate::sink::{
        LogSinker, SinkCommitCoordinator, SinkCommittedEpochSubscriber, SinkLogReader,
    };

    pub fn err_feature_not_enabled(sink_name: &'static str) -> SinkError {
        SinkError::Config(anyhow!(
            "RisingWave is not compiled with feature `sink-{}`",
            sink_name
        ))
    }

    /// Implement this trait will bring a dummy `impl Sink` for the type which always returns an error.
    pub trait FeatureNotEnabledSinkMarker: Send + 'static {
        const SINK_NAME: &'static str;
    }

    pub struct FeatureNotEnabledCoordinator<S: FeatureNotEnabledSinkMarker>(PhantomData<S>);
    #[async_trait::async_trait]
    impl<S: FeatureNotEnabledSinkMarker> SinkCommitCoordinator for FeatureNotEnabledCoordinator<S> {
        async fn init(&mut self, _subscriber: SinkCommittedEpochSubscriber) -> Result<Option<u64>> {
            Err(err_feature_not_enabled(S::SINK_NAME))
        }

        async fn commit(&mut self, _epoch: u64, _metadata: Vec<SinkMetadata>) -> Result<()> {
            Err(err_feature_not_enabled(S::SINK_NAME))
        }
    }

    pub struct FeatureNotEnabledLogSinker<S: FeatureNotEnabledSinkMarker>(PhantomData<S>);
    #[async_trait::async_trait]
    impl<S: FeatureNotEnabledSinkMarker> LogSinker for FeatureNotEnabledLogSinker<S> {
        async fn consume_log_and_sink(self, _log_reader: impl SinkLogReader) -> Result<!> {
            Err(err_feature_not_enabled(S::SINK_NAME))
        }
    }

    pub struct FeatureNotEnabledSink<S: FeatureNotEnabledSinkMarker>(PhantomData<S>);

    impl<S: FeatureNotEnabledSinkMarker> Debug for FeatureNotEnabledSink<S> {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("FeatureNotEnabledSink")
                .field("sink_name", &S::SINK_NAME)
                .finish()
        }
    }

    impl<S: FeatureNotEnabledSinkMarker> TryFrom<SinkParam> for FeatureNotEnabledSink<S> {
        type Error = SinkError;

        fn try_from(_value: SinkParam) -> std::result::Result<Self, Self::Error> {
            Err(err_feature_not_enabled(S::SINK_NAME))
        }
    }

    impl<S: FeatureNotEnabledSinkMarker> EnforceSecret for FeatureNotEnabledSink<S> {
        const ENFORCE_SECRET_PROPERTIES: Set<&'static str> = phf_set! {};

        fn enforce_secret<'a>(_prop_iter: impl Iterator<Item = &'a str>) -> ConnectorResult<()> {
            Err(err_feature_not_enabled(S::SINK_NAME).into())
        }

        fn enforce_one(_prop: &str) -> ConnectorResult<()> {
            Err(err_feature_not_enabled(S::SINK_NAME).into())
        }
    }

    impl<S: FeatureNotEnabledSinkMarker> Sink for FeatureNotEnabledSink<S> {
        type Coordinator = FeatureNotEnabledCoordinator<S>;
        type LogSinker = FeatureNotEnabledLogSinker<S>;

        const SINK_NAME: &'static str = S::SINK_NAME;

        async fn new_log_sinker(&self, _writer_param: SinkWriterParam) -> Result<Self::LogSinker> {
            Err(err_feature_not_enabled(S::SINK_NAME))
        }

        fn validate_alter_config(_config: &BTreeMap<String, String>) -> Result<()> {
            Err(err_feature_not_enabled(S::SINK_NAME))
        }

        async fn validate(&self) -> Result<()> {
            Err(err_feature_not_enabled(S::SINK_NAME))
        }

        fn is_coordinated_sink(&self) -> bool {
            true
        }

        async fn new_coordinator(
            &self,
            _db: DatabaseConnection,
            _iceberg_compact_stat_sender: Option<UnboundedSender<IcebergSinkCompactionUpdate>>,
        ) -> Result<Self::Coordinator> {
            Err(err_feature_not_enabled(S::SINK_NAME))
        }
    }
}
