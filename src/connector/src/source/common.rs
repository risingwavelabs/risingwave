// Copyright 2024 RisingWave Labs
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
use std::collections::HashMap;

use futures::{Stream, StreamExt, TryStreamExt};
use futures_async_stream::try_stream;
use risingwave_common::array::StreamChunk;

use crate::error::{ConnectorError, ConnectorResult};
use crate::parser::ParserConfig;
use crate::source::{SourceContextRef, SourceMessage};

/// Utility function to convert [`SourceMessage`] stream (got from specific connector's [`SplitReader`](super::SplitReader))
/// into [`StreamChunk`] stream (by invoking [`ByteStreamSourceParserImpl`](crate::parser::ByteStreamSourceParserImpl)).
#[try_stream(boxed, ok = StreamChunk, error = ConnectorError)]
pub(crate) async fn into_chunk_stream(
    data_stream: impl Stream<Item = ConnectorResult<Vec<SourceMessage>>> + Send + 'static,
    parser_config: ParserConfig,
    source_ctx: SourceContextRef,
) {
    let actor_id = source_ctx.actor_id.to_string();
    let fragment_id = source_ctx.fragment_id.to_string();
    let source_id = source_ctx.source_id.to_string();
    let source_name = source_ctx.source_name.to_string();
    let metrics = source_ctx.metrics.clone();
    let mut partition_input_count = HashMap::new();
    let mut partition_bytes_count = HashMap::new();

    // add metrics to the data stream
    let data_stream = data_stream
        .inspect_ok(move |data_batch| {
            let mut by_split_id = std::collections::HashMap::new();

            for msg in data_batch {
                let split_id: String = msg.split_id.as_ref().to_string();
                by_split_id
                    .entry(split_id.clone())
                    .or_insert_with(Vec::new)
                    .push(msg);
                partition_input_count
                    .entry(split_id.clone())
                    .or_insert_with(|| {
                        metrics.partition_input_count.with_guarded_label_values(&[
                            &actor_id,
                            &source_id,
                            &split_id.clone(),
                            &source_name,
                            &fragment_id,
                        ])
                    });
                partition_bytes_count
                    .entry(split_id.clone())
                    .or_insert_with(|| {
                        metrics.partition_input_bytes.with_guarded_label_values(&[
                            &actor_id,
                            &source_id,
                            &split_id,
                            &source_name,
                            &fragment_id,
                        ])
                    });
            }
            for (split_id, msgs) in by_split_id {
                partition_input_count
                    .get_mut(&split_id)
                    .unwrap()
                    .inc_by(msgs.len() as u64);

                let sum_bytes = msgs
                    .iter()
                    .flat_map(|msg| msg.payload.as_ref().map(|p| p.len() as u64))
                    .sum();

                partition_bytes_count
                    .get_mut(&split_id)
                    .unwrap()
                    .inc_by(sum_bytes);
            }
        })
        .boxed();

    let parser =
        crate::parser::ByteStreamSourceParserImpl::create(parser_config, source_ctx).await?;
    #[for_await]
    for chunk in parser.into_stream(data_stream) {
        yield chunk?;
    }
}
