// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use bytes::Bytes;
use itertools::Itertools;
use serde::{Deserialize, Serialize};

use crate::kafka::source::KafkaSplitReader;
use crate::kinesis::source::reader::KinesisSplitReader;
use crate::nexmark::source::reader::NexmarkSplitReader;

pub enum SourceOffset {
    Number(i64),
    String(String),
}

use crate::kafka::enumerator::KafkaSplitEnumerator;
use crate::kafka::KafkaSplit;
use crate::kinesis::split::KinesisSplit;
use crate::nexmark::{NexmarkSplit, NexmarkSplitEnumerator};
use crate::pulsar::{PulsarSplit, PulsarSplitEnumerator};
use crate::utils::AnyhowProperties;
use crate::{kafka, kinesis, nexmark, pulsar, Properties};

const KAFKA_SOURCE: &str = "kafka";
const KINESIS_SOURCE: &str = "kinesis";
const PULSAR_SOURCE: &str = "pulsar";
const NEXMARK_SOURCE: &str = "nexmark";

/// The message pumped from the external source service.
/// The third-party message structs will eventually be transformed into this struct.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct SourceMessage {
    pub payload: Option<Bytes>,
    pub offset: String,
    pub split_id: String,
}

/// The metadata of a split.
pub trait SourceSplit: Sized {
    fn id(&self) -> String;
    fn to_string(&self) -> Result<String>;
    fn restore_from_bytes(bytes: &[u8]) -> Result<Self>;
}

/// The persistent state of the connector.
#[derive(Debug, Clone)]
pub struct ConnectorState {
    pub identifier: Bytes,
    pub start_offset: String,
    pub end_offset: String,
}

#[derive(Debug, Clone)]
pub enum ConnectorStateV2 {
    State(ConnectorState),
    Splits(Vec<SplitImpl>),
    None,
}

#[async_trait]
pub trait SplitReader {
    async fn next(&mut self) -> Result<Option<Vec<SourceMessage>>>;
    /// `state` is used to recover from the formerly persisted state.
    /// If the reader is newly created via CREATE SOURCE, the state will be none.
    async fn new(properties: Properties, state: ConnectorStateV2) -> Result<Self>
    where
        Self: Sized;
}

pub enum SplitReaderImpl {
    Kafka(KafkaSplitReader),
    Kinesis(KinesisSplitReader),
    Nexmark(NexmarkSplitReader),
}

impl SplitReaderImpl {
    pub async fn next(&mut self) -> Result<Option<Vec<SourceMessage>>> {
        match self {
            Self::Kafka(r) => r.next().await,
            Self::Kinesis(r) => r.next().await,
            Self::Nexmark(r) => r.next().await,
        }
    }

    pub async fn create(config: Properties, state: ConnectorStateV2) -> Result<Self> {
        let upstream_type = config.get_connector_type()?;
        let connector = match upstream_type.as_str() {
            KAFKA_SOURCE => Self::Kafka(KafkaSplitReader::new(config, state).await?),
            KINESIS_SOURCE => Self::Kinesis(KinesisSplitReader::new(config, state).await?),
            NEXMARK_SOURCE => Self::Nexmark(NexmarkSplitReader::new(config, state).await?),
            _other => {
                todo!()
            }
        };
        Ok(connector)
    }
}

/// `SplitEnumerator` fetches the split metadata from the external source service.
/// NOTE: It runs in the meta server, so probably it should be moved to the `meta` crate.
#[async_trait]
pub trait SplitEnumerator {
    type Split: SourceSplit + Send + Sync;
    async fn list_splits(&mut self) -> Result<Vec<Self::Split>>;
}

pub enum SplitEnumeratorImpl {
    Kafka(kafka::enumerator::KafkaSplitEnumerator),
    Pulsar(pulsar::enumerator::PulsarSplitEnumerator),
    Kinesis(kinesis::enumerator::client::KinesisSplitEnumerator),
    Nexmark(nexmark::enumerator::NexmarkSplitEnumerator),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SplitImpl {
    Kafka(kafka::KafkaSplit),
    Pulsar(pulsar::PulsarSplit),
    Kinesis(kinesis::split::KinesisSplit),
    Nexmark(nexmark::NexmarkSplit),
}

const PULSAR_SPLIT_TYPE: &str = "pulsar";
const S3_SPLIT_TYPE: &str = "s3";
const KINESIS_SPLIT_TYPE: &str = "kinesis";
const KAFKA_SPLIT_TYPE: &str = "kafka";
const NEXMARK_SPLIT_TYPE: &str = "nexmark";

impl SplitImpl {
    pub fn id(&self) -> String {
        match self {
            SplitImpl::Kafka(k) => k.id(),
            SplitImpl::Pulsar(p) => p.id(),
            SplitImpl::Kinesis(k) => k.id(),
            SplitImpl::Nexmark(n) => n.id(),
        }
    }

    pub fn to_string(&self) -> Result<String> {
        match self {
            SplitImpl::Kafka(k) => k.to_string(),
            SplitImpl::Pulsar(p) => p.to_string(),
            SplitImpl::Kinesis(k) => k.to_string(),
            SplitImpl::Nexmark(n) => n.to_string(),
        }
    }

    pub fn get_type(&self) -> String {
        match self {
            SplitImpl::Kafka(_) => KAFKA_SPLIT_TYPE,
            SplitImpl::Pulsar(_) => PULSAR_SPLIT_TYPE,
            SplitImpl::Kinesis(_) => PULSAR_SPLIT_TYPE,
            SplitImpl::Nexmark(_) => NEXMARK_SPLIT_TYPE,
        }
        .to_string()
    }

    pub fn restore_from_bytes(split_type: String, bytes: &[u8]) -> Result<Self> {
        match split_type.as_str() {
            KAFKA_SPLIT_TYPE => KafkaSplit::restore_from_bytes(bytes).map(SplitImpl::Kafka),
            PULSAR_SPLIT_TYPE => PulsarSplit::restore_from_bytes(bytes).map(SplitImpl::Pulsar),
            KINESIS_SPLIT_TYPE => KinesisSplit::restore_from_bytes(bytes).map(SplitImpl::Kinesis),
            NEXMARK_SPLIT_TYPE => NexmarkSplit::restore_from_bytes(bytes).map(SplitImpl::Nexmark),
            other => Err(anyhow!("split type {} not supported", other)),
        }
    }
}

impl SplitEnumeratorImpl {
    pub async fn list_splits(&mut self) -> Result<Vec<SplitImpl>> {
        match self {
            SplitEnumeratorImpl::Kafka(k) => k
                .list_splits()
                .await
                .map(|ss| ss.into_iter().map(SplitImpl::Kafka).collect_vec()),
            SplitEnumeratorImpl::Pulsar(p) => p
                .list_splits()
                .await
                .map(|ss| ss.into_iter().map(SplitImpl::Pulsar).collect_vec()),
            SplitEnumeratorImpl::Kinesis(k) => k
                .list_splits()
                .await
                .map(|ss| ss.into_iter().map(SplitImpl::Kinesis).collect_vec()),
            SplitEnumeratorImpl::Nexmark(k) => k
                .list_splits()
                .await
                .map(|ss| ss.into_iter().map(SplitImpl::Nexmark).collect_vec()),
        }
    }

    pub fn create(properties: &AnyhowProperties) -> Result<SplitEnumeratorImpl> {
        let source_type = properties.get_connector_type()?;
        match source_type.as_str() {
            KAFKA_SOURCE => KafkaSplitEnumerator::new(properties).map(SplitEnumeratorImpl::Kafka),
            PULSAR_SOURCE => {
                PulsarSplitEnumerator::new(properties).map(SplitEnumeratorImpl::Pulsar)
            }
            KINESIS_SOURCE => todo!(),
            NEXMARK_SOURCE => {
                NexmarkSplitEnumerator::new(properties).map(SplitEnumeratorImpl::Nexmark)
            }
            _ => Err(anyhow!("unsupported source type: {}", source_type)),
        }
    }
}
