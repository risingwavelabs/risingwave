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

#![expect(dead_code)]
#![allow(clippy::derive_partial_eq_without_eq)]
#![feature(generators)]
#![feature(proc_macro_hygiene)]
#![feature(stmt_expr_attributes)]
#![feature(box_patterns)]
#![feature(trait_alias)]
#![feature(binary_heap_drain_sorted)]
#![feature(lint_reasons)]
#![feature(once_cell)]
#![feature(result_option_inspect)]
#![feature(let_chains)]
#![feature(box_into_inner)]
#![feature(type_alias_impl_trait)]

use std::collections::HashMap;

use futures::stream::BoxStream;
use risingwave_common::array::StreamChunk;
use risingwave_common::error::RwError;
use source::SplitId;

pub mod aws_utils;
pub mod error;
mod macros;
mod manager;
pub use manager::SourceColumnDesc;
pub mod parser;
pub mod sink;
pub mod source;

#[derive(Clone, Debug, Default)]
pub struct ConnectorParams {
    pub connector_rpc_endpoint: Option<String>,
}

impl ConnectorParams {
    pub fn new(connector_rpc_endpoint: Option<String>) -> Self {
        Self {
            connector_rpc_endpoint,
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
