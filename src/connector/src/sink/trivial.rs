// Copyright 2025 RisingWave Labs
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

use std::marker::PhantomData;
use std::sync::Arc;

use async_trait::async_trait;
use phf::{Set, phf_set};
use risingwave_common::session_config::sink_decouple::SinkDecouple;

use crate::enforce_secret::EnforceSecret;
use crate::sink::log_store::{LogStoreReadItem, TruncateOffset};
use crate::sink::{LogSinker, Result, Sink, SinkError, SinkLogReader, SinkParam, SinkWriterParam};

pub const BLACKHOLE_SINK: &str = "blackhole";
pub const TABLE_SINK: &str = "table";

pub trait TrivialSinkType: Send + 'static {
    /// Whether to enable debug log for every item to sink.
    const DEBUG_LOG: bool;
    const SINK_NAME: &'static str;
}

#[derive(Debug)]
pub struct BlackHole;

impl TrivialSinkType for BlackHole {
    const DEBUG_LOG: bool = true;
    const SINK_NAME: &'static str = BLACKHOLE_SINK;
}

pub type BlackHoleSink = TrivialSink<BlackHole>;

#[derive(Debug)]
pub struct Table;

impl TrivialSinkType for Table {
    const DEBUG_LOG: bool = false;
    const SINK_NAME: &'static str = TABLE_SINK;
}

pub type TableSink = TrivialSink<Table>;

#[derive(Debug)]
pub struct TrivialSink<T: TrivialSinkType> {
    param: Arc<SinkParam>,
    _marker: PhantomData<T>,
}

impl<T: TrivialSinkType> EnforceSecret for TrivialSink<T> {
    const ENFORCE_SECRET_PROPERTIES: Set<&'static str> = phf_set! {};
}

impl<T: TrivialSinkType> TryFrom<SinkParam> for TrivialSink<T> {
    type Error = SinkError;

    fn try_from(param: SinkParam) -> std::result::Result<Self, Self::Error> {
        Ok(Self {
            param: Arc::new(param),
            _marker: PhantomData,
        })
    }
}

impl<T: TrivialSinkType> Sink for TrivialSink<T> {
    type LogSinker = Self;

    const SINK_NAME: &'static str = T::SINK_NAME;

    /// Enable sink decoupling for sink-into-table.
    /// Disable sink decoupling for blackhole sink. It introduces overhead without any benefit
    fn is_sink_decouple(user_specified: &SinkDecouple) -> Result<bool> {
        match user_specified {
            SinkDecouple::Enable => Ok(true),
            SinkDecouple::Default | SinkDecouple::Disable => Ok(false),
        }
    }

    fn support_schema_change() -> bool {
        true
    }

    async fn new_log_sinker(&self, _writer_env: SinkWriterParam) -> Result<Self::LogSinker> {
        Ok(Self {
            param: self.param.clone(),
            _marker: PhantomData,
        })
    }

    async fn validate(&self) -> Result<()> {
        Ok(())
    }
}

#[async_trait]
impl<T: TrivialSinkType> LogSinker for TrivialSink<T> {
    async fn consume_log_and_sink(self, mut log_reader: impl SinkLogReader) -> Result<!> {
        let schema = self.param.schema();

        log_reader.start_from(None).await?;
        loop {
            let (epoch, item) = log_reader.next_item().await?;
            match item {
                LogStoreReadItem::StreamChunk { chunk_id, chunk } => {
                    if T::DEBUG_LOG {
                        tracing::debug!(
                            target: "events::sink::message::chunk",
                            sink_id = %self.param.sink_id,
                            sink_name = self.param.sink_name,
                            cardinality = chunk.cardinality(),
                            capacity = chunk.capacity(),
                            "\n{}\n", chunk.to_pretty_with_schema(&schema),
                        );
                    }

                    log_reader.truncate(TruncateOffset::Chunk { epoch, chunk_id })?;
                }
                LogStoreReadItem::Barrier { .. } => {
                    if T::DEBUG_LOG {
                        tracing::debug!(
                            target: "events::sink::message::barrier",
                            sink_id = %self.param.sink_id,
                            sink_name = self.param.sink_name,
                            epoch,
                        );
                    }

                    log_reader.truncate(TruncateOffset::Barrier { epoch })?;
                }
            }
        }
    }
}
