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

use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Result};
use async_trait::async_trait;

use crate::nexmark::config::NexmarkConfig;
use crate::nexmark::source::event::EventType;
use crate::nexmark::source::generator::NexmarkEventGenerator;
use crate::nexmark::{NexmarkProperties, NexmarkSplit};
use crate::{Column, ConnectorState, SourceMessage, SplitImpl, SplitMetaData, SplitReader};

#[derive(Clone, Debug)]
pub struct NexmarkSplitReader {
    generator: NexmarkEventGenerator,
    assigned_split: Option<NexmarkSplit>,
}

#[async_trait]
impl SplitReader for NexmarkSplitReader {
    type Properties = Box<NexmarkProperties>;

    async fn new(
        properties: Box<NexmarkProperties>,
        state: ConnectorState,
        _columns: Option<Vec<Column>>,
    ) -> Result<Self>
    where
        Self: Sized,
    {
        let properties = *properties;

        let wall_clock_base_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as usize;

        let event_type_string = properties.table_type.clone();

        let event_type = match event_type_string.as_str() {
            "Person" => EventType::Person,
            "Auction" => EventType::Auction,
            "Bid" => EventType::Bid,
            _ => return Err(anyhow!("Unknown table type {} found", event_type_string)),
        };

        let use_real_time = properties.use_real_time;
        let mut min_event_gap_in_ns = 0;
        if !use_real_time {
            min_event_gap_in_ns = properties.min_event_gap_in_ns;
        }

        let max_chunk_size = properties.max_chunk_size;
        let event_num = properties.event_num;

        let mut generator = NexmarkEventGenerator {
            config: Box::new(NexmarkConfig::from(properties)?),
            wall_clock_base_time,
            events_so_far: 0,
            event_num,
            split_index: 0,
            split_num: 0,
            split_id: String::new(),
            last_event: None,
            event_type,
            use_real_time,
            min_event_gap_in_ns,
            max_chunk_size,
        };

        let mut assigned_split = NexmarkSplit::default();

        if let Some(splits) = state {
            log::debug!("Splits for nexmark found! {:?}", splits);
            for split in splits {
                // TODO: currently, assume there's only on split in one reader
                let split_id = split.id();
                if let SplitImpl::Nexmark(n) = split {
                    generator.split_index = n.split_index;
                    generator.split_num = n.split_num;
                    if let Some(s) = n.start_offset {
                        generator.events_so_far = s;
                    };
                    generator.split_id = split_id;
                    assigned_split = n;
                    break;
                }
            }
        }

        Ok(Self {
            generator,
            assigned_split: Some(assigned_split),
        })
    }

    async fn next(&mut self) -> Result<Option<Vec<SourceMessage>>> {
        let chunk = match self.generator.next().await {
            Err(e) => return Err(anyhow!(e)),
            Ok(chunk) => chunk,
        };

        Ok(Some(chunk))
    }
}

impl NexmarkSplitReader {}
#[cfg(test)]
mod tests {
    use anyhow::Result;

    use super::*;
    use crate::nexmark::NexmarkSplitEnumerator;
    use crate::{SplitEnumerator, SplitImpl};

    #[tokio::test]
    async fn test_nexmark_split_reader() -> Result<()> {
        let props = NexmarkProperties {
            split_num: Some(2),
            min_event_gap_in_ns: 0,
            table_type: "Bid".to_string(),
            max_chunk_size: 5,
            ..Default::default()
        };

        let mut enumerator = NexmarkSplitEnumerator::new(Box::new(props.clone())).await?;
        let list_splits_resp = enumerator
            .list_splits()
            .await?
            .into_iter()
            .map(SplitImpl::Nexmark)
            .collect();

        let state = Some(list_splits_resp);
        let mut reader = NexmarkSplitReader::new(Box::new(props), state, None).await?;
        let chunk = reader.next().await?.unwrap();
        assert_eq!(chunk.len(), 5);

        Ok(())
    }
}
