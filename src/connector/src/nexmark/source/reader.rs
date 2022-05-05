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
use crate::nexmark::{
    NexmarkSplit, NEXMARK_CONFIG_EVENT_NUM, NEXMARK_CONFIG_MAX_CHUNK_SIZE,
    NEXMARK_CONFIG_MIN_EVENT_GAP_IN_NS, NEXMARK_CONFIG_TABLE_TYPE, NEXMARK_CONFIG_USE_REAL_TIME,
    NEXMARK_MAX_FETCH_MESSAGES,
};
use crate::{ConnectorStateV2, Properties, SourceMessage, SplitImpl, SplitReader};

#[derive(Clone, Debug)]
pub struct NexmarkSplitReader {
    generator: NexmarkEventGenerator,
    assigned_split: Option<NexmarkSplit>,
}

#[async_trait]
impl SplitReader for NexmarkSplitReader {
    async fn next(&mut self) -> Result<Option<Vec<SourceMessage>>> {
        let chunk = match self.generator.next().await {
            Err(e) => return Err(anyhow!(e)),
            Ok(chunk) => chunk,
        };

        Ok(Some(chunk))
    }

    async fn new(properties: Properties, state: ConnectorStateV2) -> Result<Self>
    where
        Self: Sized,
    {
        let wall_clock_base_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as usize;

        let event_type_string = properties.get_nexmark(NEXMARK_CONFIG_TABLE_TYPE)?;

        let event_type = match event_type_string.as_str() {
            "Person" => EventType::Person,
            "Auction" => EventType::Auction,
            "Bid" => EventType::Bid,
            _ => return Err(anyhow!("Unknown table type {} found", event_type_string)),
        };

        let use_real_time = properties.get_as_or(NEXMARK_CONFIG_USE_REAL_TIME, false)?;
        let mut min_event_gap_in_ns = 0;

        if !use_real_time {
            min_event_gap_in_ns =
                properties.get_as_or(NEXMARK_CONFIG_MIN_EVENT_GAP_IN_NS, 100000)?;
        }

        let max_chunk_size =
            properties.get_as_or(NEXMARK_CONFIG_MAX_CHUNK_SIZE, NEXMARK_MAX_FETCH_MESSAGES)?;

        let event_num = properties.get_as_or(NEXMARK_CONFIG_EVENT_NUM, -1)?;

        let mut generator = NexmarkEventGenerator {
            config: Box::new(NexmarkConfig::from(&properties)?),
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

        match state {
            ConnectorStateV2::Splits(splits) => {
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
            ConnectorStateV2::State(cs) => {
                log::debug!("Splits for nexmark found! {:?}", cs);
                todo!()
            }
            ConnectorStateV2::None => {}
        }

        Ok(Self {
            generator,
            assigned_split: Some(assigned_split),
        })
    }
}
#[cfg(test)]
mod tests {
    use anyhow::Result;
    use maplit::hashmap;

    use super::*;
    use crate::nexmark::{
        NexmarkSplitEnumerator, NEXMARK_CONFIG_MIN_EVENT_GAP_IN_NS, NEXMARK_CONFIG_SPLIT_NUM,
        NEXMARK_CONFIG_TABLE_TYPE,
    };
    use crate::{AnyhowProperties, Properties, SplitEnumerator, SplitImpl};

    #[tokio::test]
    async fn test_nexmark_split_reader() -> Result<()> {
        let any_how_properties = AnyhowProperties::new(
            hashmap! {NEXMARK_CONFIG_SPLIT_NUM.to_string() => "2".to_string()},
        );
        let mut enumerator = NexmarkSplitEnumerator::new(&any_how_properties)?;
        let list_splits_resp = enumerator
            .list_splits()
            .await?
            .into_iter()
            .map(SplitImpl::Nexmark)
            .collect();

        let state = ConnectorStateV2::Splits(list_splits_resp);
        let properties = Properties::new(hashmap! {
            NEXMARK_CONFIG_MIN_EVENT_GAP_IN_NS.to_string() => "0".to_string(),
            NEXMARK_CONFIG_TABLE_TYPE.to_string() => "Bid".to_string(),
            NEXMARK_CONFIG_MAX_CHUNK_SIZE.to_string() => "5".to_string(),
        });
        let mut reader = NexmarkSplitReader::new(properties, state).await?;
        let chunk = reader.next().await?.unwrap();
        assert_eq!(chunk.len(), 5);

        Ok(())
    }
}
