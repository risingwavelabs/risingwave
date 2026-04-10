// Copyright 2022 RisingWave Labs
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
    #[serde(default)]
    pub(crate) id: Option<SplitId>,
    /// Note: currently the start offset is **exclusive**. We need to `+1` to create the reader.
    /// Possible values are:
    /// - `Earliest`: `low_watermark` - 1
    /// - `Latest`: `high_watermark` - 1
    /// - `Timestamp`: `offset_for_timestamp` - 1
    /// - `last_seen_offset`
    ///
    /// A better approach would be to make it **inclusive**. <https://github.com/risingwavelabs/risingwave/pull/16257>
    pub(crate) start_offset: Option<i64>,
    pub(crate) stop_offset: Option<i64>,
}

impl SplitMetaData for KafkaSplit {
    fn id(&self) -> SplitId {
        self.id
            .clone()
            .unwrap_or_else(|| self.partition.to_string().into())
    }

    fn restore_from_json(value: JsonbVal) -> ConnectorResult<Self> {
        serde_json::from_value(value.take()).map_err(Into::into)
    }

    fn encode_to_json(&self) -> JsonbVal {
        serde_json::to_value(self.clone()).unwrap().into()
    }

    fn update_offset(&mut self, last_seen_offset: String) -> ConnectorResult<()> {
        self.start_offset = Some(last_seen_offset.as_str().parse::<i64>().unwrap());
        Ok(())
    }
}

impl KafkaSplit {
    pub fn regex_split_id(&self) -> SplitId {
        format!("{}-{}", self.topic, self.partition).into()
    }

    pub fn new(
        partition: i32,
        start_offset: Option<i64>,
        stop_offset: Option<i64>,
        topic: String,
    ) -> KafkaSplit {
        KafkaSplit {
            id: None,
            topic,
            partition,
            start_offset,
            stop_offset,
        }
    }

    pub fn with_legacy_id(mut self) -> Self {
        self.id = None;
        self
    }

    pub fn with_split_id(mut self, split_id: SplitId) -> Self {
        self.id = Some(split_id);
        self
    }

    pub fn get_topic_and_partition(&self) -> (String, i32) {
        (self.topic.clone(), self.partition)
    }

    pub fn start_offset(&self) -> Option<i64> {
        self.start_offset
    }
}
