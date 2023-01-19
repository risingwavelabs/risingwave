use std::collections::HashMap;

// Copyright 2023 Singularity Data
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
use futures::stream::BoxStream;

pub mod base;
pub mod cdc;
pub mod datagen;
pub mod dummy_connector;
pub mod filesystem;
pub mod google_pubsub;
pub mod kafka;
pub mod kinesis;
pub mod monitor;
pub mod nexmark;
pub mod pulsar;
pub use base::*;
pub use google_pubsub::GOOGLE_PUBSUB_CONNECTOR;
pub use kafka::KAFKA_CONNECTOR;
pub use kinesis::KINESIS_CONNECTOR;
use risingwave_common::array::StreamChunk;
use risingwave_common::catalog::TableId;
mod manager;
pub use manager::SourceColumnDesc;
use risingwave_common::error::RwError;

pub use crate::source::nexmark::NEXMARK_CONNECTOR;
pub use crate::source::pulsar::PULSAR_CONNECTOR;

#[derive(Clone, Copy, Debug, Default)]
pub struct SourceInfo {
    pub actor_id: u32,
    pub source_id: TableId,
}

impl SourceInfo {
    pub fn new(actor_id: u32, source_id: TableId) -> Self {
        SourceInfo {
            actor_id,
            source_id,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SourceFormat {
    Invalid,
    Json,
    Protobuf,
    DebeziumJson,
    Avro,
    Maxwell,
    CanalJson,
    Csv,
}

pub type BoxSourceWithStateStream = BoxStream<'static, Result<StreamChunkWithState, RwError>>;

/// [`StreamChunkWithState`] returns stream chunk together with offset for each split. In the
/// current design, one connector source can have multiple split reader. The keys are unique
/// `split_id` and values are the latest offset for each split.
#[derive(Clone, Debug)]
pub struct StreamChunkWithState {
    pub chunk: StreamChunk,
    pub split_offset_mapping: Option<HashMap<SplitId, String>>,
}

/// The `split_offset_mapping` field is unused for the table source, so we implement `From` for it.
impl From<StreamChunk> for StreamChunkWithState {
    fn from(chunk: StreamChunk) -> Self {
        Self {
            chunk,
            split_offset_mapping: None,
        }
    }
}
