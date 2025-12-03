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

use async_nats::jetstream::consumer;
use async_trait::async_trait;
use futures::StreamExt;
use futures_async_stream::try_stream;
use risingwave_common::bail;
use risingwave_common::metrics::LabelGuardedIntGauge;

use super::message::NatsMessage;
use super::{NatsOffset, NatsSplit};
use crate::error::ConnectorResult as Result;
use crate::parser::ParserConfig;
use crate::source::common::into_chunk_stream;
use crate::source::nats::NatsProperties;
use crate::source::{
    BoxSourceChunkStream, Column, SourceContextRef, SourceMessage, SplitId, SplitReader,
};

pub struct NatsSplitReader {
    consumer: consumer::Consumer<consumer::pull::Config>,
    #[expect(dead_code)]
    properties: NatsProperties,
    parser_config: ParserConfig,
    source_ctx: SourceContextRef,
    #[expect(dead_code)]
    start_position: NatsOffset,
    split_id: SplitId,
    // Tracks the number of active NATS connections. When this guard is dropped,
    // the metric is automatically decremented.
    _connection_metric: LabelGuardedIntGauge,
}

#[async_trait]
impl SplitReader for NatsSplitReader {
    type Properties = NatsProperties;
    type Split = NatsSplit;

    async fn new(
        properties: NatsProperties,
        splits: Vec<NatsSplit>,
        parser_config: ParserConfig,
        source_ctx: SourceContextRef,
        _columns: Option<Vec<Column>>,
    ) -> Result<Self> {
        // We guarantee the split num always align with parallelism
        assert_eq!(splits.len(), 1);
        let split = splits.into_iter().next().unwrap();
        let split_id = split.split_id;
        let start_position = match &split.start_sequence {
            NatsOffset::None => match &properties.scan_startup_mode {
                None => NatsOffset::Earliest,
                Some(mode) => match mode.as_str() {
                    "latest" => NatsOffset::Latest,
                    "earliest" => NatsOffset::Earliest,
                    "timestamp" | "timestamp_millis" /* backward-compat */ => {
                        if let Some(ts) = &properties.start_timestamp_millis {
                            NatsOffset::Timestamp(*ts)
                        } else {
                            bail!("scan.startup.timestamp.millis is required");
                        }
                    }
                    _ => {
                        bail!("invalid scan.startup.mode, accept earliest/latest/timestamp")
                    }
                },
            },
            // We have record on this Nats Split, contains the last seen offset (seq id) or reply subject
            // We do not use the seq id as start position anymore,
            // but just let the reader load from durable consumer on broker.
            start_position => start_position.to_owned(),
        };

        let mut config = consumer::pull::Config {
            ..Default::default()
        };
        properties.set_config(&mut config);

        let consumer = properties
            .common
            .build_consumer(
                properties.stream.clone(),
                properties.durable_consumer_name.clone(),
                split_id.to_string(),
                start_position.clone(),
                config,
            )
            .await?;

        // Create a guarded metric to track the number of NATS connections.
        // The metric is incremented here and will be automatically decremented when dropped.
        let connection_metric = source_ctx
            .metrics
            .nats_source_connections
            .with_guarded_label_values(&[
                &source_ctx.source_id.to_string(),
                &source_ctx.source_name,
                &source_ctx.fragment_id.to_string(),
            ]);
        connection_metric.inc();

        Ok(Self {
            consumer,
            properties,
            parser_config,
            source_ctx,
            start_position,
            split_id,
            _connection_metric: connection_metric,
        })
    }

    fn into_stream(self) -> BoxSourceChunkStream {
        let parser_config = self.parser_config.clone();
        let source_context = self.source_ctx.clone();
        into_chunk_stream(self.into_data_stream(), parser_config, source_context)
    }
}

impl NatsSplitReader {
    #[try_stream(ok = Vec<SourceMessage>, error = crate::error::ConnectorError)]
    async fn into_data_stream(self) {
        let capacity = self.source_ctx.source_ctrl_opts.chunk_size;
        let messages = self.consumer.messages().await?;
        #[for_await]
        for msgs in messages.ready_chunks(capacity) {
            let mut msg_vec = Vec::with_capacity(capacity);
            for msg in msgs {
                msg_vec.push(SourceMessage::from(NatsMessage::new(
                    self.split_id.clone(),
                    msg?,
                )));
            }
            yield msg_vec;
        }
    }
}
