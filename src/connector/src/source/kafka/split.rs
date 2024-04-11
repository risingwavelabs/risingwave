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

use risingwave_common::types::JsonbVal;
use serde::{Deserialize, Serialize};

use crate::error::ConnectorResult;
use crate::source::{SplitId, SplitMetaData};

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Hash)]
pub struct KafkaSplit {
    pub(crate) topic: String,
    pub(crate) partition: i32,

    /// The offset pointing to the first message to read from the partition. Inclusive.
    pub(crate) start_offset: Option<i64>,
    /// The offset pointing to the next position of the last message to read from the partition. Exclusive.
    pub(crate) stop_offset: Option<i64>,

    /// The version of the split metadata. `None` means version 0.
    version: Option<u32>,
}

/// The current version of the split metadata.
/// Please increment this version and write a migration script below when you make a change to
/// the split metadata.
const CURRENT_VERSION: u32 = 1;

static MIGRATION_SCRIPTS: &[fn(serde_json::Value) -> serde_json::Value] =
    &[/* 0: 0 -> 1 */ migration::v0_to_v1];

mod migration {
    pub fn v0_to_v1(mut value: serde_json::Value) -> serde_json::Value {
        // In v0, the `start_offset` field represented the last seen offset, which means when
        // initializing a reader from the split, we should add 1 to the offset. But in the case
        // of timestamp offset, it happened that we forgot to subtract 1 from the offset
        // when creating `KafkaSplit`, causing the first message to be missed.
        // See https://github.com/risingwavelabs/risingwave/issues/16046 for more.
        //
        // In v1, we normalize the `start_offset` field to represent the first message to read
        // from the partition. So we need to add 1 to any v0 `start_offset` field. We also
        // normalize the `stop_offset` field to represent the next position of the last message,
        // while fortunately, although not documented, the v0 `stop_offset` field was already
        // representing the next position of the last message.

        let start_offset = value["start_offset"]
            .as_i64()
            .map(|start_offset| start_offset + 1);
        value["start_offset"] = start_offset.into();
        value
    }
}

impl SplitMetaData for KafkaSplit {
    fn id(&self) -> SplitId {
        // TODO: should avoid constructing a string every time
        format!("{}", self.partition).into()
    }

    fn restore_from_json(value: JsonbVal) -> ConnectorResult<Self> {
        let mut value = value.take();
        let mut version: u32 = value["version"].as_u64().unwrap_or(0) as u32;
        while version < CURRENT_VERSION {
            value = MIGRATION_SCRIPTS[version as usize](value);
            version += 1;
        }
        value["version"] = CURRENT_VERSION.into();
        serde_json::from_value(value).map_err(Into::into)
    }

    fn encode_to_json(&self) -> JsonbVal {
        serde_json::to_value(self.clone()).unwrap().into()
    }

    fn update_offset(&mut self, last_seen_offset: String) -> ConnectorResult<()> {
        let last_seen_offset = last_seen_offset.as_str().parse::<i64>().unwrap();
        self.start_offset = Some(last_seen_offset + 1);
        Ok(())
    }
}

impl KafkaSplit {
    pub fn new(
        topic: String,
        partition: i32,
        start_offset: Option<i64>,
        stop_offset: Option<i64>,
    ) -> KafkaSplit {
        KafkaSplit {
            topic,
            partition,
            start_offset,
            stop_offset,
            version: Some(CURRENT_VERSION),
        }
    }

    pub fn get_topic_and_partition(&self) -> (String, i32) {
        (self.topic.clone(), self.partition)
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::types::JsonbVal;

    use super::KafkaSplit;
    use crate::source::SplitMetaData;

    #[test]
    fn test_migration_from_v0_to_v1() {
        let test = serde_json::json!({"partition": 0, "start_offset": 2, "stop_offset": null, "topic": "test"});
        let value = JsonbVal::from(test);

        let split = KafkaSplit::restore_from_json(value).unwrap();
        assert_eq!(split.start_offset, Some(3));
        assert_eq!(split.version, Some(1));
    }
}
