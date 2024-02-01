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

use futures::{Stream, StreamExt, TryStreamExt};
use futures_async_stream::try_stream;
use risingwave_common::array::StreamChunk;
use risingwave_common::error::RwError;

use crate::parser::ParserConfig;
use crate::source::{SourceContextRef, SourceMessage, SplitReader};

pub(crate) trait CommonSplitReader: SplitReader + 'static {
    fn into_data_stream(
        self,
    ) -> impl Stream<Item = Result<Vec<SourceMessage>, anyhow::Error>> + Send;
}

#[try_stream(boxed, ok = StreamChunk, error = RwError)]
pub(crate) async fn into_chunk_stream(
    reader: impl CommonSplitReader,
    parser_config: ParserConfig,
    source_ctx: SourceContextRef,
) {
    let actor_id = source_ctx.source_info.actor_id.to_string();
    let fragment_id = source_ctx.source_info.fragment_id.to_string();
    let source_id = source_ctx.source_info.source_id.to_string();
    let source_name = source_ctx.source_info.source_name.to_string();
    let metrics = source_ctx.metrics.clone();

    let data_stream = reader.into_data_stream();

    let data_stream = data_stream
        .inspect_ok(move |data_batch| {
            let mut by_split_id = std::collections::HashMap::new();

            for msg in data_batch {
                by_split_id
                    .entry(msg.split_id.as_ref())
                    .or_insert_with(Vec::new)
                    .push(msg);
            }

            for (split_id, msgs) in by_split_id {
                metrics
                    .partition_input_count
                    .with_label_values(&[
                        &actor_id,
                        &source_id,
                        split_id,
                        &source_name,
                        &fragment_id,
                    ])
                    .inc_by(msgs.len() as u64);

                let sum_bytes = msgs
                    .iter()
                    .flat_map(|msg| msg.payload.as_ref().map(|p| p.len() as u64))
                    .sum();

                metrics
                    .partition_input_bytes
                    .with_label_values(&[
                        &actor_id,
                        &source_id,
                        split_id,
                        &source_name,
                        &fragment_id,
                    ])
                    .inc_by(sum_bytes);
            }
        })
        .boxed();

    let parser =
        crate::parser::ByteStreamSourceParserImpl::create(parser_config, source_ctx).await?;
    #[for_await]
    for msg_batch in parser.into_stream(data_stream) {
        yield msg_batch?;
    }
}
