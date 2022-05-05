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

#![warn(clippy::dbg_macro)]
#![warn(clippy::disallowed_methods)]
#![warn(clippy::doc_markdown)]
#![warn(clippy::explicit_into_iter_loop)]
#![warn(clippy::explicit_iter_loop)]
#![warn(clippy::inconsistent_struct_constructor)]
#![warn(clippy::map_flatten)]
#![warn(clippy::no_effect_underscore_binding)]
#![warn(clippy::await_holding_lock)]
#![deny(unused_must_use)]
#![deny(rustdoc::broken_intra_doc_links)]
#![feature(trait_alias)]
#![feature(generic_associated_types)]
#![feature(binary_heap_drain_sorted)]
#![feature(mutex_unlock)]

use std::fmt::Debug;

use async_trait::async_trait;
use enum_as_inner::EnumAsInner;
pub use manager::*;
pub use parser::*;
use risingwave_common::array::StreamChunk;
use risingwave_common::error::Result;
pub use table_v2::*;

use crate::connector_source::{ConnectorSource, ConnectorStreamReader};

pub mod parser;

pub mod connector_source;
mod manager;

mod common;
mod row_id;
mod table_v2;

extern crate core;
extern crate maplit;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SourceFormat {
    Invalid,
    Json,
    Protobuf,
    DebeziumJson,
    Avro,
}

#[derive(Debug, EnumAsInner)]
pub enum SourceImpl {
    TableV2(TableSourceV2),
    Connector(ConnectorSource),
}

#[allow(clippy::large_enum_variant)]
pub enum SourceStreamReaderImpl {
    TableV2(TableV2StreamReader),
    Connector(ConnectorStreamReader),
}

#[async_trait]
impl StreamSourceReader for SourceStreamReaderImpl {
    async fn next(&mut self) -> Result<StreamChunk> {
        match self {
            SourceStreamReaderImpl::TableV2(t) => t.next().await,
            SourceStreamReaderImpl::Connector(c) => c.next().await,
        }
    }
}

#[async_trait]
pub trait StreamSourceReader: Send + Sync + 'static {
    /// `next` always returns a StreamChunk. If the queue is empty, it will
    /// block until new data coming
    async fn next(&mut self) -> Result<StreamChunk>;
}
