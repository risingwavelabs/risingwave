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

use std::collections::HashMap;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use bytes::Bytes;
use itertools::Itertools;
use serde::{Deserialize, Serialize};

use crate::dummy_connector::DummySplitReader;
use crate::kafka::source::KafkaSplitReader;
use crate::kinesis::source::reader::KinesisSplitReader;
use crate::kinesis::split::KinesisOffset;
use crate::nexmark::source::reader::NexmarkSplitReader;

pub enum SourceOffset {
    Number(i64),
    String(String),
}

use crate::kafka::enumerator::KafkaSplitEnumerator;
use crate::kafka::KafkaSplit;
use crate::kinesis::split::KinesisSplit;
use crate::nexmark::{NexmarkSplit, NexmarkSplitEnumerator};
use crate::pulsar::source::reader::PulsarSplitReader;
use crate::pulsar::{PulsarEnumeratorOffset, PulsarSplit, PulsarSplitEnumerator};
use crate::{kafka, kinesis, nexmark, pulsar, ConnectorProperties};

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
pub trait SplitMetaData: Sized {
    fn id(&self) -> String;
    fn to_json_bytes(&self) -> Bytes;
    fn restore_from_bytes(bytes: &[u8]) -> Result<Self>;
}

/// The persistent state of the connector.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectorState {
    pub identifier: Bytes,
    pub start_offset: String,
    pub end_offset: String,
}

impl ConnectorState {
    pub fn from_hashmap(state: HashMap<String, String>) -> Vec<Self> {
        if state.is_empty() {
            return vec![];
        }
        let mut connector_states: Vec<Self> = Vec::with_capacity(state.len());
        connector_states.extend(state.iter().map(|(split, offset)| Self {
            identifier: Bytes::from(split.to_owned()),
            start_offset: offset.clone(),
            end_offset: "".to_string(),
        }));
        connector_states
    }
}

impl From<SplitImpl> for ConnectorState {
    fn from(split: SplitImpl) -> Self {
        match split {
            SplitImpl::Kafka(kafka) => Self {
                identifier: Bytes::from(kafka.partition.to_string()),
                start_offset: kafka.start_offset.unwrap().to_string(),
                end_offset: if let Some(end_offset) = kafka.stop_offset {
                    end_offset.to_string()
                } else {
                    "".to_string()
                },
            },
            SplitImpl::Kinesis(kinesis) => Self {
                identifier: Bytes::from(kinesis.shard_id),
                start_offset: match kinesis.start_position {
                    KinesisOffset::SequenceNumber(s) => s,
                    _ => "".to_string(),
                },
                end_offset: match kinesis.end_position {
                    KinesisOffset::SequenceNumber(s) => s,
                    _ => "".to_string(),
                },
            },
            SplitImpl::Pulsar(pulsar) => Self {
                identifier: Bytes::from(pulsar.topic.to_string()),
                start_offset: match pulsar.start_offset {
                    PulsarEnumeratorOffset::MessageId(id) => id,
                    _ => "".to_string(),
                },
                end_offset: "".to_string(),
            },
            SplitImpl::Nexmark(nex_mark) => Self {
                identifier: Bytes::from(nex_mark.id()),
                start_offset: match nex_mark.start_offset {
                    Some(s) => s.to_string(),
                    _ => "".to_string(),
                },
                end_offset: "".to_string(),
            },
        }
    }
}

impl SplitMetaData for ConnectorState {
    fn id(&self) -> String {
        String::from_utf8(self.identifier.to_vec()).unwrap()
    }

    fn to_json_bytes(&self) -> Bytes {
        Bytes::from(serde_json::to_string(self).unwrap())
    }

    fn restore_from_bytes(bytes: &[u8]) -> Result<Self> {
        serde_json::from_slice(bytes).map_err(|e| anyhow!(e))
    }
}

#[derive(Debug, Clone)]
pub enum ConnectorStateV2 {
    // ConnectorState should change to Vec<ConnectorState> because there can be multiple readers
    // in a source executor
    State(ConnectorState),
    Splits(Vec<SplitImpl>),
    None,
}

#[async_trait]
pub trait SplitReader {
    async fn next(&mut self) -> Result<Option<Vec<SourceMessage>>>;
}

pub enum SplitReaderImpl {
    Kafka(KafkaSplitReader),
    Kinesis(KinesisSplitReader),
    Dummy(DummySplitReader),
    Nexmark(NexmarkSplitReader),
    Pulsar(PulsarSplitReader),
}

impl SplitReaderImpl {
    pub async fn next(&mut self) -> Result<Option<Vec<SourceMessage>>> {
        match self {
            Self::Kafka(r) => r.next().await,
            Self::Kinesis(r) => r.next().await,
            Self::Dummy(r) => r.next().await,
            Self::Nexmark(r) => r.next().await,
            Self::Pulsar(r) => r.next().await,
        }
    }

    pub async fn create(config: ConnectorProperties, state: ConnectorStateV2) -> Result<Self> {
        if let ConnectorStateV2::Splits(s) = &state {
            if s.is_empty() {
                return Ok(Self::Dummy(DummySplitReader {}));
            }
        }

        let connector = match config {
            ConnectorProperties::Kafka(props) => {
                Self::Kafka(KafkaSplitReader::new(props, state).await?)
            }
            ConnectorProperties::Kinesis(props) => {
                Self::Kinesis(KinesisSplitReader::new(props, state).await?)
            }
            ConnectorProperties::Nexmark(props) => {
                Self::Nexmark(NexmarkSplitReader::new(*props, state).await?)
            }
            ConnectorProperties::Pulsar(props) => {
                Self::Pulsar(PulsarSplitReader::new(props, state).await?)
            }
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
    type Split: SplitMetaData + Send + Sync;
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

    pub fn to_json_bytes(&self) -> Bytes {
        match self {
            SplitImpl::Kafka(k) => k.to_json_bytes(),
            SplitImpl::Pulsar(p) => p.to_json_bytes(),
            SplitImpl::Kinesis(k) => k.to_json_bytes(),
            SplitImpl::Nexmark(n) => n.to_json_bytes(),
        }
    }

    pub fn get_type(&self) -> String {
        match self {
            SplitImpl::Kafka(_) => KAFKA_SPLIT_TYPE,
            SplitImpl::Pulsar(_) => PULSAR_SPLIT_TYPE,
            SplitImpl::Kinesis(_) => KINESIS_SPLIT_TYPE,
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

    pub fn create(properties: ConnectorProperties) -> Result<Self> {
        match properties {
            ConnectorProperties::Kafka(props) => KafkaSplitEnumerator::new(props).map(Self::Kafka),
            ConnectorProperties::Pulsar(props) => {
                PulsarSplitEnumerator::new(props).map(Self::Pulsar)
            }
            ConnectorProperties::Kinesis(_) => todo!(),
            ConnectorProperties::S3(_) => todo!(),
            ConnectorProperties::Nexmark(props) => {
                NexmarkSplitEnumerator::new(&props).map(Self::Nexmark)
            }
        }
    }
}
