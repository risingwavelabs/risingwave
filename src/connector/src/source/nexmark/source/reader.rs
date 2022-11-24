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

use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use futures::StreamExt;
use futures_async_stream::try_stream;
use itertools::Itertools;
use nexmark::config::NexmarkConfig;
use nexmark::event::EventType;
use nexmark::EventGenerator;
use tokio::time::Instant;

use crate::source::nexmark::source::message::NexmarkMessage;
use crate::source::nexmark::{NexmarkProperties, NexmarkSplit};
use crate::source::{
    spawn_data_generation_stream, BoxSourceStream, Column, ConnectorState, SourceMessage, SplitId,
    SplitMetaData, SplitReader,
};

#[derive(Debug)]
pub struct NexmarkSplitReader {
    generator: EventGenerator,
    assigned_split: NexmarkSplit,
    split_id: SplitId,
    split_index: i32,
    split_num: i32,
    event_num: i64,
    event_type: EventType,
    use_real_time: bool,
    min_event_gap_in_ns: u64,
    max_chunk_size: u64,
}

#[async_trait]
impl SplitReader for NexmarkSplitReader {
    type Properties = NexmarkProperties;

    async fn new(
        properties: NexmarkProperties,
        state: ConnectorState,
        _columns: Option<Vec<Column>>,
    ) -> Result<Self> {
        let mut assigned_split = NexmarkSplit::default();
        let mut split_id = "".into();
        let mut split_index = 0;
        let mut split_num = 0;
        let mut events_so_far = 0;

        if let Some(splits) = state {
            tracing::debug!("Splits for nexmark found! {:?}", splits);
            // TODO: currently, assume there's only one split in one reader
            let split = splits.into_iter().exactly_one().unwrap();
            split_id = split.id();
            let split = split.into_nexmark().unwrap();

            split_index = split.split_index;
            split_num = split.split_num;
            if let Some(s) = split.start_offset {
                events_so_far = s;
            };
            assigned_split = split;
        }

        Ok(NexmarkSplitReader {
            generator: EventGenerator::new(NexmarkConfig::from(&*properties))
                .with_events_so_far(events_so_far)
                .with_type_filter(properties.table_type),
            assigned_split,
            split_id,
            split_index,
            split_num,
            max_chunk_size: properties.max_chunk_size,
            event_num: properties.event_num,
            event_type: properties.table_type,
            use_real_time: properties.use_real_time,
            min_event_gap_in_ns: properties.min_event_gap_in_ns,
        })
    }

    fn into_stream(self) -> BoxSourceStream {
        // Will buffer at most 4 event chunks.
        const BUFFER_SIZE: usize = 4;
        spawn_data_generation_stream(self.into_stream(), BUFFER_SIZE).boxed()
    }
}

impl NexmarkSplitReader {
    #[try_stream(boxed, ok = Vec<SourceMessage>, error = anyhow::Error)]
    async fn into_stream(mut self) {
        let mut last_event = None;
        let wall_clock_base_time = Instant::now();
        loop {
            let mut msgs: Vec<SourceMessage> = vec![];
            let old_events_so_far = self.generator.events_so_far();

            if let Some(event) = last_event.take() {
                msgs.push(event);
            }

            let mut finished = false;

            while (msgs.len() as u64) < self.max_chunk_size {
                let event = self.generator.next().unwrap();

                if self.event_num > 0 && self.generator.events_so_far() > self.event_num as u64 {
                    finished = true;
                    break;
                }

                if self.generator.events_so_far() % self.split_num as u64 != self.split_index as u64
                {
                    continue;
                }

                let event = NexmarkMessage::new(
                    self.split_id.clone(),
                    self.generator.events_so_far(),
                    event,
                );

                // When the generated timestamp is larger then current timestamp, if its the first
                // event, sleep and continue. Otherwise, directly return.
                if self.use_real_time {
                    tokio::time::sleep_until(wall_clock_base_time + self.generator.elapsed()).await;

                    last_event = Some(event.into());
                    break;
                }

                msgs.push(event.into());
            }

            if finished && msgs.is_empty() {
                break;
            } else {
                yield msgs;
            }

            if !self.use_real_time && self.min_event_gap_in_ns > 0 {
                tokio::time::sleep(Duration::from_nanos(
                    (self.generator.events_so_far() - old_events_so_far) * self.min_event_gap_in_ns,
                ))
                .await;
            }
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
            table_type: EventType::Bid,
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
            let state = Some(vec![split]);
            let mut reader = NexmarkSplitReader::new(props.clone(), state, None)
                .await?
                .into_stream();
            let _chunk = reader.next().await.unwrap()?;
        }

        Ok(())
    }
}
