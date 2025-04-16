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

use std::time::Duration;

use async_trait::async_trait;
use futures::{StreamExt, TryStreamExt};
use futures_async_stream::try_stream;
use nexmark::EventGenerator;
use nexmark::config::NexmarkConfig;
use nexmark::event::EventType;
use risingwave_common::array::stream_chunk_builder::StreamChunkBuilder;
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{DataType, ScalarImpl};
use risingwave_common_estimate_size::EstimateSize;
use tokio::time::Instant;

use crate::error::ConnectorResult;
use crate::parser::ParserConfig;
use crate::source::data_gen_util::spawn_data_generation_stream;
use crate::source::nexmark::source::combined_event::{
    combined_event_to_row, event_to_row, get_event_data_types, new_combined_event,
};
use crate::source::nexmark::{NexmarkProperties, NexmarkSplit};
use crate::source::{
    BoxSourceChunkStream, Column, SourceContextRef, SplitId, SplitMetaData, SplitReader,
};

#[derive(Debug)]
pub struct NexmarkSplitReader {
    generator: EventGenerator,
    #[expect(dead_code)]
    assigned_split: NexmarkSplit,
    event_num: u64,
    event_type: Option<EventType>,
    use_real_time: bool,
    min_event_gap_in_ns: u64,
    max_chunk_size: u64,

    row_id_index: Option<usize>,
    split_id: SplitId,
    source_ctx: SourceContextRef,
}

#[async_trait]
impl SplitReader for NexmarkSplitReader {
    type Properties = NexmarkProperties;
    type Split = NexmarkSplit;

    #[allow(clippy::unused_async)]
    async fn new(
        properties: NexmarkProperties,
        splits: Vec<NexmarkSplit>,
        parser_config: ParserConfig,
        source_ctx: SourceContextRef,
        _columns: Option<Vec<Column>>,
    ) -> ConnectorResult<Self> {
        tracing::debug!("Splits for nexmark found! {:?}", splits);
        assert!(splits.len() == 1);
        // TODO: currently, assume there's only one split in one reader
        let split = splits.into_iter().next().unwrap();
        let split_id = split.id();

        let split_index = split.split_index as u64;
        let split_num = split.split_num as u64;
        let offset = split.start_offset.unwrap_or(split_index);
        let assigned_split = split;

        let mut generator = EventGenerator::new(NexmarkConfig::from(&properties))
            .with_offset(offset)
            .with_step(split_num);
        // If the user doesn't specify the event type in the source definition, then the user
        // intends to use unified source(three different types of events together).
        if let Some(event_type) = properties.table_type.as_ref() {
            generator = generator.with_type_filter(*event_type);
        }

        let row_id_index = parser_config
            .common
            .rw_columns
            .into_iter()
            .position(|column| column.is_row_id());

        Ok(NexmarkSplitReader {
            generator,
            assigned_split,
            split_id,
            max_chunk_size: properties.max_chunk_size,
            event_num: properties.event_num,
            event_type: properties.table_type,
            use_real_time: properties.use_real_time,
            min_event_gap_in_ns: properties.min_event_gap_in_ns,
            row_id_index,
            source_ctx,
        })
    }

    fn into_stream(self) -> BoxSourceChunkStream {
        let actor_id = self.source_ctx.actor_id.to_string();
        let fragment_id = self.source_ctx.fragment_id.to_string();
        let source_id = self.source_ctx.source_id.to_string();
        let source_name = self.source_ctx.source_name.clone();
        let split_id = self.split_id.clone();
        let metrics = self.source_ctx.metrics.clone();

        let labels: &[&str] = &[&actor_id, &source_id, &split_id, &source_name, &fragment_id];
        let partition_input_count_metric = metrics
            .partition_input_count
            .with_guarded_label_values(labels);
        let partition_input_bytes_metric = metrics
            .partition_input_bytes
            .with_guarded_label_values(labels);

        // Will buffer at most 4 event chunks.
        const BUFFER_SIZE: usize = 4;
        spawn_data_generation_stream(
            self.into_native_stream()
                .inspect_ok(move |chunk: &StreamChunk| {
                    partition_input_count_metric.inc_by(chunk.cardinality() as u64);
                    partition_input_bytes_metric.inc_by(chunk.estimated_size() as u64);
                }),
            BUFFER_SIZE,
        )
        .boxed()
    }
}

impl NexmarkSplitReader {
    async fn sleep_til_next_event(&self, start_time: Instant, start_offset: u64, start_ts: u64) {
        if self.use_real_time {
            tokio::time::sleep_until(
                start_time + Duration::from_millis(self.generator.timestamp() - start_ts),
            )
            .await;
        } else if self.min_event_gap_in_ns > 0 {
            tokio::time::sleep_until(
                start_time
                    + Duration::from_nanos(
                        self.min_event_gap_in_ns * (self.generator.global_offset() - start_offset),
                    ),
            )
            .await;
        }
    }

    #[try_stream(boxed, ok = StreamChunk, error = crate::error::ConnectorError)]
    async fn into_native_stream(mut self) {
        let start_time = Instant::now();
        let start_offset = self.generator.global_offset();
        let start_ts = self.generator.timestamp();
        let mut event_dtypes_with_offset = get_event_data_types(self.event_type, self.row_id_index);
        event_dtypes_with_offset.extend([DataType::Varchar, DataType::Varchar]);

        let mut chunk_builder =
            StreamChunkBuilder::new(self.max_chunk_size as usize, event_dtypes_with_offset);
        loop {
            if self.generator.global_offset() >= self.event_num {
                break;
            }
            let event = self.generator.next().unwrap();
            let mut fields = match self.event_type {
                Some(_) => event_to_row(event, self.row_id_index),
                None => combined_event_to_row(new_combined_event(event), self.row_id_index),
            };
            fields.extend([
                Some(ScalarImpl::Utf8(self.split_id.as_ref().into())),
                Some(ScalarImpl::Utf8(
                    self.generator.offset().to_string().into_boxed_str(),
                )),
            ]);

            if let Some(chunk) = chunk_builder.append_row(Op::Insert, OwnedRow::new(fields)) {
                self.sleep_til_next_event(start_time, start_offset, start_ts)
                    .await;
                yield chunk;
            }
        }

        if let Some(chunk) = chunk_builder.take() {
            self.sleep_til_next_event(start_time, start_offset, start_ts)
                .await;
            yield chunk;
        }

        tracing::debug!(?self.event_type, "nexmark generator finished");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::source::nexmark::NexmarkSplitEnumerator;
    use crate::source::{SourceContext, SourceEnumeratorContext, SplitEnumerator};

    #[tokio::test]
    async fn test_nexmark_split_reader() -> crate::error::ConnectorResult<()> {
        let props = NexmarkProperties {
            split_num: 2,
            min_event_gap_in_ns: 0,
            table_type: Some(EventType::Bid),
            max_chunk_size: 5,
            ..Default::default()
        };

        let mut enumerator =
            NexmarkSplitEnumerator::new(props.clone(), SourceEnumeratorContext::dummy().into())
                .await?;
        let list_splits_resp: Vec<_> = enumerator.list_splits().await?.into_iter().collect();

        assert_eq!(list_splits_resp.len(), 2);

        for split in list_splits_resp {
            let state = vec![split];
            let mut reader = NexmarkSplitReader::new(
                props.clone(),
                state,
                Default::default(),
                SourceContext::dummy().into(),
                None,
            )
            .await?
            .into_stream();
            let _chunk = reader.next().await.unwrap()?;
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_nexmark_event_num() -> crate::error::ConnectorResult<()> {
        let max_chunk_size = 32;
        let event_num = max_chunk_size * 128 + 1;
        let props = NexmarkProperties {
            split_num: 1,
            min_event_gap_in_ns: 0,
            table_type: None,
            max_chunk_size,
            event_num,
            ..Default::default()
        };

        let mut enumerator =
            NexmarkSplitEnumerator::new(props.clone(), SourceEnumeratorContext::dummy().into())
                .await?;
        let list_splits_resp: Vec<_> = enumerator.list_splits().await?.into_iter().collect();

        for split in list_splits_resp {
            let state = vec![split];
            let reader = NexmarkSplitReader::new(
                props.clone(),
                state,
                Default::default(),
                SourceContext::dummy().into(),
                None,
            )
            .await?
            .into_stream();
            let (rows_count, chunk_count) = reader
                .fold((0, 0), |acc, x| async move {
                    (acc.0 + x.unwrap().cardinality(), acc.1 + 1)
                })
                .await;
            assert_eq!(rows_count as u64, event_num);
            assert_eq!(chunk_count as u64, event_num / max_chunk_size + 1);
        }

        Ok(())
    }
}
