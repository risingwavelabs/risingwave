<<<<<<< HEAD
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
//
use anyhow::Result;
use std::collections::{BTreeMap, HashMap};
use std::hash::Hash;

use anyhow::{anyhow, Error};


use async_trait::async_trait;
use bytes::Bytes;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use kafka::enumerator::KafkaSplitEnumerator;

pub enum SourceOffset {
    Number(i64),
    String(String),
}

use crate::kafka;
use crate::pulsar;


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

#[async_trait]
pub trait SourceReader: Sized {
    async fn next(&mut self) -> Result<Option<Vec<InnerMessage>>>;
    async fn assign_split<'a>(&'a mut self, split: &'a [u8]) -> Result<()>;
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
    async fn list_splits(&mut self) -> Result<Vec<SplitImpl>> {
        match self {
            SplitEnumeratorImpl::Kafka(k) => {
                k.list_splits().await.map(|ss| ss.into_iter().map(SplitImpl::Kafka).collect_vec())
            }
            SplitEnumeratorImpl::Pulsar(p) => {
                p.list_splits().await.map(|ss| ss.into_iter().map(SplitImpl::Pulsar).collect_vec())
            }
        }
    }
}

pub fn extract_split_enumerator(properties: &HashMap<String, String>) -> Result<SplitEnumeratorImpl> {
    let source_type = match properties.get("upstream.source") {
        None => return Err(anyhow!("upstream.source not found")),
        Some(value) => value,
    };

    match source_type.as_ref() {
        "kafka" => {
            KafkaSplitEnumerator::new(properties).map(SplitEnumeratorImpl::Kafka)
        }
        _ => Err(anyhow!("unsupported source type: {}", source_type)),
    }
}
