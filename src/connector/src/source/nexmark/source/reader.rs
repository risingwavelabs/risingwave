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

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use futures::{StreamExt, TryStreamExt};
use futures_async_stream::try_stream;
use nexmark::config::NexmarkConfig;
use nexmark::event::EventType;
use nexmark::EventGenerator;
use tokio::time::Instant;

use crate::impl_common_split_reader_logic;
use crate::parser::ParserConfig;
use crate::source::data_gen_util::spawn_data_generation_stream;
use crate::source::monitor::SourceMetrics;
use crate::source::nexmark::source::message::NexmarkMessage;
use crate::source::nexmark::{NexmarkProperties, NexmarkSplit};
use crate::source::{
    BoxSourceStream, BoxSourceWithStateStream, Column, SourceInfo, SourceMessage, SplitId,
    SplitImpl, SplitMetaData, SplitReaderV2,
};

impl_common_split_reader_logic!(NexmarkSplitReader, NexmarkProperties);

#[derive(Debug)]
pub struct NexmarkSplitReader {
    generator: EventGenerator,
    assigned_split: NexmarkSplit,
    event_num: u64,
    event_type: Option<EventType>,
    use_real_time: bool,
    min_event_gap_in_ns: u64,
    max_chunk_size: u64,

    split_id: SplitId,
    parser_config: ParserConfig,
    metrics: Arc<SourceMetrics>,
    source_info: SourceInfo,
}

#[async_trait]
impl SplitReaderV2 for NexmarkSplitReader {
    type Properties = NexmarkProperties;

    #[allow(clippy::unused_async)]
    async fn new(
        properties: NexmarkProperties,
        splits: Vec<SplitImpl>,
        parser_config: ParserConfig,
        metrics: Arc<SourceMetrics>,
        source_info: SourceInfo,
        _columns: Option<Vec<Column>>,
    ) -> Result<Self> {
        tracing::debug!("Splits for nexmark found! {:?}", splits);
        assert!(splits.len() == 1);
        // TODO: currently, assume there's only one split in one reader
        let split = splits.into_iter().next().unwrap().into_nexmark().unwrap();
        let split_id = split.id();

        let split_index = split.split_index as u64;
        let split_num = split.split_num as u64;
        let offset = split.start_offset.unwrap_or(split_index);
        let assigned_split = split;

        let mut generator = EventGenerator::new(NexmarkConfig::from(&*properties))
            .with_offset(offset)
            .with_step(split_num);
        // If the user doesn't specify the event type in the source definition, then the user
        // intends to use unified source(three different types of events together).
        if let Some(event_type) = properties.table_type.as_ref() {
            generator = generator.with_type_filter(*event_type);
        }

        Ok(NexmarkSplitReader {
            generator,
            assigned_split,
            split_id,
            max_chunk_size: properties.max_chunk_size,
            event_num: properties.event_num,
            event_type: properties.table_type,
            use_real_time: properties.use_real_time,
            min_event_gap_in_ns: properties.min_event_gap_in_ns,
            parser_config,
            metrics,
            source_info,
        })
    }

    fn into_stream(self) -> BoxSourceWithStateStream {
        self.into_chunk_stream()
    }
}

impl NexmarkSplitReader {
    fn into_data_stream(self) -> BoxSourceStream {
        // Will buffer at most 4 event chunks.
        const BUFFER_SIZE: usize = 4;
        spawn_data_generation_stream(self.into_data_stream_inner(), BUFFER_SIZE).boxed()
    }

    #[try_stream(boxed, ok = Vec<SourceMessage>, error = anyhow::Error)]
    async fn into_data_stream_inner(mut self) {
        let start_time = Instant::now();
        let start_offset = self.generator.global_offset();
        let start_ts = self.generator.timestamp();
        loop {
            let mut msgs: Vec<SourceMessage> = vec![];
            while (msgs.len() as u64) < self.max_chunk_size {
                if self.generator.global_offset() >= self.event_num {
                    break;
                }
                let event = self.generator.next().unwrap();
                let event = match self.event_type {
                    Some(_) => NexmarkMessage::new_single_event(
                        self.split_id.clone(),
                        self.generator.offset(),
                        event,
                    ),
                    None => NexmarkMessage::new_combined_event(
                        self.split_id.clone(),
                        self.generator.offset(),
                        event,
                    ),
                };
                msgs.push(event.into());
            }
            if msgs.is_empty() {
                break;
            }
            if self.use_real_time {
                tokio::time::sleep_until(
                    start_time + Duration::from_millis(self.generator.timestamp() - start_ts),
                )
                .await;
            } else if self.min_event_gap_in_ns > 0 {
                tokio::time::sleep_until(
                    start_time
                        + Duration::from_nanos(
                            self.min_event_gap_in_ns
                                * (self.generator.global_offset() - start_offset),
                        ),
                )
                .await;
            }
            yield msgs;
        }

        tracing::debug!(?self.event_type, "nexmark generator finished");
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result;

    use super::*;
    use crate::source::nexmark::{NexmarkPropertiesInner, NexmarkSplitEnumerator};
    use crate::source::{SplitEnumerator, SplitImpl};

    #[tokio::test]
    async fn test_nexmark_split_reader() -> Result<()> {
        let props = Box::new(NexmarkPropertiesInner {
            split_num: 2,
            min_event_gap_in_ns: 0,
            table_type: Some(EventType::Bid),
            max_chunk_size: 5,
            ..Default::default()
        });

        let mut enumerator = NexmarkSplitEnumerator::new(props.clone()).await?;
        let list_splits_resp: Vec<SplitImpl> = enumerator
            .list_splits()
            .await?
            .into_iter()
            .map(SplitImpl::Nexmark)
            .collect();

        assert_eq!(list_splits_resp.len(), 2);

        for split in list_splits_resp {
            let state = vec![split];
            let mut reader = NexmarkSplitReader::new(
                props.clone(),
                state,
                Default::default(),
                Default::default(),
                Default::default(),
                None,
            )
            .await?
            .into_data_stream();
            let _chunk = reader.next().await.unwrap()?;
        }

        Ok(())
    }
}
