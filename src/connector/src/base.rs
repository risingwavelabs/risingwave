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
use paste::paste;
use serde::{Deserialize, Serialize};

use crate::datagen::{
    DatagenProperties, DatagenSplit, DatagenSplitEnumerator, DatagenSplitReader, DATAGEN_CONNECTOR,
};
use crate::dummy_connector::DummySplitReader;
use crate::filesystem::s3::{S3Properties, S3_CONNECTOR};
use crate::kafka::enumerator::KafkaSplitEnumerator;
use crate::kafka::source::KafkaSplitReader;
use crate::kafka::{KafkaProperties, KafkaSplit, KAFKA_CONNECTOR};
use crate::kinesis::enumerator::client::KinesisSplitEnumerator;
use crate::kinesis::source::reader::KinesisMultiSplitReader;
use crate::kinesis::split::{KinesisOffset, KinesisSplit};
use crate::kinesis::{KinesisProperties, KINESIS_CONNECTOR};
use crate::nexmark::source::reader::NexmarkSplitReader;
use crate::nexmark::{NexmarkProperties, NexmarkSplit, NexmarkSplitEnumerator, NEXMARK_CONNECTOR};
use crate::pulsar::source::reader::PulsarSplitReader;
use crate::pulsar::{
    PulsarEnumeratorOffset, PulsarProperties, PulsarSplit, PulsarSplitEnumerator, PULSAR_CONNECTOR,
};
use crate::{impl_connector_properties, impl_split, impl_split_enumerator, impl_split_reader};

/// [`SplitEnumerator`] fetches the split metadata from the external source service.
/// NOTE: It runs in the meta server, so probably it should be moved to the `meta` crate.
#[async_trait]
pub trait SplitEnumerator: Sized {
    type Split: SplitMetaData + Send + Sync;
    type Properties;

    async fn new(properties: Self::Properties) -> Result<Self>;
    async fn list_splits(&mut self) -> Result<Vec<Self::Split>>;
}

/// [`SplitReader`] is an abstraction of the external connector read interface,
/// used to read messages from the outside and transform them into source-oriented
/// [`SourceMessage`], in order to improve throughput, it is recommended to return a batch of
/// messages at a time, [`Option`] is used to be compatible with the Stream API, but the stream of a
/// Streaming system should not end
#[async_trait]
pub trait SplitReader: Sized {
    type Properties;

    async fn new(
        properties: Self::Properties,
        state: ConnectorStateV2,
        columns: Option<Vec<Column>>,
    ) -> Result<Self>;
    async fn next(&mut self) -> Result<Option<Vec<SourceMessage>>>;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SplitImpl {
    Kafka(KafkaSplit),
    Pulsar(PulsarSplit),
    Kinesis(KinesisSplit),
    Nexmark(NexmarkSplit),
    Datagen(DatagenSplit),
}

pub enum SplitReaderImpl {
    Kinesis(Box<KinesisMultiSplitReader>),
    Kafka(Box<KafkaSplitReader>),
    Dummy(Box<DummySplitReader>),
    Nexmark(Box<NexmarkSplitReader>),
    Pulsar(Box<PulsarSplitReader>),
    Datagen(Box<DatagenSplitReader>),
}

pub enum SplitEnumeratorImpl {
    Kafka(KafkaSplitEnumerator),
    Pulsar(PulsarSplitEnumerator),
    Kinesis(KinesisSplitEnumerator),
    Nexmark(NexmarkSplitEnumerator),
    Datagen(DatagenSplitEnumerator),
}

#[derive(Clone, Debug, Deserialize)]
pub enum ConnectorProperties {
    Kafka(KafkaProperties),
    Pulsar(PulsarProperties),
    Kinesis(KinesisProperties),
    Nexmark(Box<NexmarkProperties>),
    Datagen(DatagenProperties),
    S3(S3Properties),
    Dummy(()),
}

impl_connector_properties! {
    [ ] ,
    { Kafka, KAFKA_CONNECTOR },
    { Pulsar, PULSAR_CONNECTOR },
    { Kinesis, KINESIS_CONNECTOR },
    { Nexmark, NEXMARK_CONNECTOR },
    { Datagen, DATAGEN_CONNECTOR },
    { S3, S3_CONNECTOR }
}

impl_split_enumerator! {
    [ ] ,
    { Kafka, KafkaSplitEnumerator },
    { Pulsar, PulsarSplitEnumerator },
    { Kinesis, KinesisSplitEnumerator },
    { Nexmark, NexmarkSplitEnumerator },
    { Datagen, DatagenSplitEnumerator }
}

impl_split! {
    [ ] ,
    { Kafka, KAFKA_CONNECTOR, KafkaSplit },
    { Pulsar, PULSAR_CONNECTOR, PulsarSplit },
    { Kinesis, KINESIS_CONNECTOR, KinesisSplit },
    { Nexmark, NEXMARK_CONNECTOR, NexmarkSplit },
    { Datagen, DATAGEN_CONNECTOR, DatagenSplit }
}

impl_split_reader! {
    [ ] ,
    { Kafka, KafkaSplitReader },
    { Pulsar, PulsarSplitReader },
    { Kinesis, KinesisMultiSplitReader },
    { Nexmark, NexmarkSplitReader },
    { Datagen, DatagenSplitReader },
    { Dummy, DummySplitReader }
}

pub type DataType = risingwave_common::types::DataType;

#[derive(Clone, Debug)]
pub struct Column {
    pub name: String,
    pub data_type: DataType,
}

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
    fn encode_to_bytes(&self) -> Bytes;
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

// TODO: use macro and trait to simplify the code here
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
            SplitImpl::Datagen(dategen) => Self {
                identifier: Bytes::from(dategen.id()),
                start_offset: "".to_string(),
                end_offset: "".to_string(),
            },
        }
    }
}

impl SplitMetaData for ConnectorState {
    fn id(&self) -> String {
        String::from_utf8(self.identifier.to_vec()).unwrap()
    }

    fn encode_to_bytes(&self) -> Bytes {
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

mod tests {
    #[allow(unused_imports)]
    use super::*;

    #[test]
    fn test_split_impl_get_fn() -> Result<()> {
        let split = KafkaSplit::new(0, Some(0), Some(0), "demo".to_string());
        let split_impl = SplitImpl::Kafka(split.clone());
        let get_value = split_impl.get_kafka()?;
        println!("{:?}", get_value);
        assert_eq!(split.encode_to_bytes(), get_value.encode_to_bytes());

        Ok(())
    }
}
