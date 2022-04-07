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

#![allow(dead_code)]
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

use std::collections::HashMap;
use std::fmt::Debug;

use async_trait::async_trait;
use connector_source::ConnectorSource;
use enum_as_inner::EnumAsInner;
pub use high_level_kafka::*;
pub use manager::*;
pub use parser::*;
use risingwave_common::array::{DataChunk, StreamChunk};
use risingwave_common::catalog::ColumnId;
use risingwave_common::error::Result;
pub use table_v2::*;

pub mod parser;

pub mod connector_source;
mod high_level_kafka;
mod manager;

mod common;
mod table_v2;

extern crate maplit;

#[derive(Clone, Debug)]
pub enum SourceConfig {
    Kafka(HighLevelKafkaSourceConfig),
    Connector(HashMap<String, String>),
}

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
    HighLevelKafka(HighLevelKafkaSource),
    TableV2(TableSourceV2),
    Connector(ConnectorSource),
}

#[async_trait]
pub trait Source: Send + Sync + 'static {
    type ReaderContext;
    type BatchReader: BatchSourceReader;
    type StreamReader: StreamSourceReader;

    /// Create a batch reader
    fn batch_reader(
        &self,
        context: Self::ReaderContext,
        column_ids: Vec<ColumnId>,
    ) -> Result<Self::BatchReader>;

    /// Create a stream reader
    fn stream_reader(
        &self,
        context: Self::ReaderContext,
        column_ids: Vec<ColumnId>,
    ) -> Result<Self::StreamReader>;
}

#[async_trait]
pub trait BatchSourceReader: Send + Sync + 'static {
    /// `open` is called once to initialize the reader
    async fn open(&mut self) -> Result<()>;

    /// `next` returns a row or `None` immediately if there is no more result
    async fn next(&mut self) -> Result<Option<DataChunk>>;

    /// `close` is called to stop the reader
    async fn close(&mut self) -> Result<()>;
}

#[async_trait]
pub trait StreamSourceReader: Send + Sync + 'static {
    /// `init` is called once to initialize the reader
    async fn open(&mut self) -> Result<()>;

    /// `next` always returns a StreamChunk. If the queue is empty, it will
    /// block until new data coming
    async fn next(&mut self) -> Result<StreamChunk>;
}
