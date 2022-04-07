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
use kafka::enumerator::KafkaSplitEnumerator;
use serde::{Deserialize, Serialize};

use crate::kafka::source::KafkaSplitReader;
use crate::kinesis::source::reader::KinesisSplitReader;

pub enum SourceOffset {
    Number(i64),
    String(String),
}

use crate::pulsar::PulsarSplitEnumerator;
use crate::{kafka, pulsar};

const UPSTREAM_SOURCE_KEY: &str = "connector";
const KAFKA_SOURCE: &str = "kafka";
const KINESIS_SOURCE: &str = "kinesis";

pub trait SourceMessage {
    fn payload(&self) -> Result<Option<&[u8]>>;
    fn offset(&self) -> Result<Option<SourceOffset>>;
    fn serialize(&self) -> Result<String>;
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct InnerMessage {
    pub payload: Option<Bytes>,
    pub offset: String,
    pub split_id: String,
}

pub trait SourceSplit {
    fn id(&self) -> String;
    fn to_string(&self) -> Result<String>;
}

#[derive(Debug, Clone)]
pub struct ConnectorState {
    pub identifier: Bytes,
    pub start_offset: String,
    pub end_offset: String,
}

#[async_trait]
pub trait SourceReader {
    async fn next(&mut self) -> Result<Option<Vec<InnerMessage>>>;
    // async fn assign_split<'a>(&'a mut self, split: &'a [u8]) -> Result<()>;
    async fn new(config: HashMap<String, String>, state: Option<ConnectorState>) -> Result<Self>
    where
        Self: Sized;
}

#[async_trait]
pub trait SplitEnumerator {
    type Split: SourceSplit + Send + Sync;
    async fn list_splits(&mut self) -> Result<Vec<Self::Split>>;
}

pub enum SplitEnumeratorImpl {
    Kafka(KafkaSplitEnumerator),
    Pulsar(pulsar::enumerator::PulsarSplitEnumerator),
}

pub enum SplitImpl {
    Kafka(kafka::KafkaSplit),
    Pulsar(pulsar::PulsarSplit),
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
        }
    }
}

pub fn extract_split_enumerator(
    properties: &HashMap<String, String>,
) -> Result<SplitEnumeratorImpl> {
    let source_type = match properties.get("upstream.source") {
        None => return Err(anyhow!("upstream.source not found")),
        Some(value) => value,
    };

    match source_type.as_ref() {
        "kafka" => KafkaSplitEnumerator::new(properties).map(SplitEnumeratorImpl::Kafka),
        "pulsar" => PulsarSplitEnumerator::new(properties).map(SplitEnumeratorImpl::Pulsar),
        _ => Err(anyhow!("unsupported source type: {}", source_type)),
    }
}

pub async fn new_connector(
    config: HashMap<String, String>,
    state: Option<ConnectorState>,
) -> Result<Box<dyn SourceReader + Send + Sync>> {
    let upstream_type = config.get(UPSTREAM_SOURCE_KEY).unwrap();
    let connector: Box<dyn SourceReader + Send + Sync> = match upstream_type.as_str() {
        KAFKA_SOURCE => {
            let kafka = KafkaSplitReader::new(config, state).await?;
            Box::new(kafka)
        }
        KINESIS_SOURCE => {
            let kinesis = KinesisSplitReader::new(config, state).await?;
            Box::new(kinesis)
        }
        _other => {
            todo!()
        }
    };
    Ok(connector)
}
