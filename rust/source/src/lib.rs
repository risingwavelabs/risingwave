#![allow(dead_code)]
#![warn(clippy::disallowed_methods)]
#![warn(clippy::doc_markdown)]
#![warn(clippy::explicit_into_iter_loop)]
#![warn(clippy::explicit_iter_loop)]
#![warn(clippy::inconsistent_struct_constructor)]
#![warn(clippy::map_flatten)]
#![warn(clippy::no_effect_underscore_binding)]
#![warn(clippy::await_holding_lock)]
#![feature(trait_alias)]
#![feature(generic_associated_types)]
#![feature(binary_heap_drain_sorted)]

use std::fmt::Debug;

use async_trait::async_trait;
pub use high_level_kafka::*;
pub use manager::*;
pub use parser::*;
use risingwave_common::array::{DataChunk, StreamChunk};
use risingwave_common::catalog::ColumnId;
use risingwave_common::error::Result;
pub use table_v2::*;

pub mod parser;

mod high_level_kafka;
mod manager;

mod common;
mod table_v2;

extern crate maplit;

#[derive(Clone, Debug)]
pub enum SourceConfig {
    Kafka(HighLevelKafkaSourceConfig),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SourceFormat {
    Invalid,
    Json,
    Protobuf,
    DebeziumJson,
    Avro,
}

#[derive(Debug)]
pub enum SourceImpl {
    HighLevelKafka(HighLevelKafkaSource),
    TableV2(TableSourceV2),
}

impl SourceImpl {
    pub fn as_table_v2(&self) -> &TableSourceV2 {
        match self {
            SourceImpl::TableV2(table) => table,
            _ => panic!("not a table source v2"),
        }
    }
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
