// Copyright 2023 RisingWave Labs
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

use async_trait::async_trait;

use crate::sink::log_store::{LogReader, LogStoreReadItem, TruncateOffset};
use crate::sink::{
    DummySinkCommitCoordinator, LogSinker, Result, Sink, SinkError, SinkParam, SinkWriterParam,
};

pub const TABLE_SINK: &str = "table";

/// A table sink outputs stream into another RisingWave's table.
///
/// Different from a materialized view, table sinks do not enforce strong consistency between upstream and downstream in principle. As a result, the `create sink` statement returns immediately, which is similar to any other `create sink`. It also allows users to execute DMLs on these target tables.
///
/// See also [RFC: Create Sink into Table](https://github.com/risingwavelabs/rfcs/pull/52).
#[derive(Debug)]
pub struct TableSink;

impl TryFrom<SinkParam> for TableSink {
    type Error = SinkError;

    fn try_from(_value: SinkParam) -> std::result::Result<Self, Self::Error> {
        Ok(Self)
    }
}

impl Sink for TableSink {
    type Coordinator = DummySinkCommitCoordinator;
    type LogSinker = Self;

    const SINK_NAME: &'static str = TABLE_SINK;

    async fn new_log_sinker(&self, _writer_env: SinkWriterParam) -> Result<Self::LogSinker> {
        Ok(Self)
    }

    async fn validate(&self) -> Result<()> {
        Ok(())
    }
}

#[async_trait]
impl LogSinker for TableSink {
    async fn consume_log_and_sink(self, mut log_reader: impl LogReader) -> Result<()> {
        log_reader.init().await?;
        loop {
            let (epoch, item) = log_reader.next_item().await?;
            match item {
                LogStoreReadItem::StreamChunk { chunk_id, .. } => {
                    log_reader
                        .truncate(TruncateOffset::Chunk { epoch, chunk_id })
                        .await?;
                }
                LogStoreReadItem::Barrier { .. } => {
                    log_reader
                        .truncate(TruncateOffset::Barrier { epoch })
                        .await?;
                }
                LogStoreReadItem::UpdateVnodeBitmap(_) => {}
            }
        }
    }
}
