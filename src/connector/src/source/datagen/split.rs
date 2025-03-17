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

use risingwave_common::types::JsonbVal;
use serde::{Deserialize, Serialize};

use crate::error::ConnectorResult;
use crate::source::SplitId;
use crate::source::base::SplitMetaData;

#[derive(Clone, Serialize, Deserialize, Debug, Default, PartialEq, Hash)]
pub struct DatagenSplit {
    pub split_index: i32,
    pub split_num: i32,
    pub start_offset: Option<u64>,
}

impl SplitMetaData for DatagenSplit {
    fn id(&self) -> SplitId {
        // TODO: should avoid constructing a string every time
        format!("{}-{}", self.split_num, self.split_index).into()
    }

    fn restore_from_json(value: JsonbVal) -> ConnectorResult<Self> {
        serde_json::from_value(value.take()).map_err(Into::into)
    }

    fn encode_to_json(&self) -> JsonbVal {
        serde_json::to_value(self.clone()).unwrap().into()
    }

    fn update_offset(&mut self, last_seen_offset: String) -> ConnectorResult<()> {
        self.start_offset = Some(last_seen_offset.as_str().parse::<u64>().unwrap());
        Ok(())
    }
}

impl DatagenSplit {
    pub fn new(split_index: i32, split_num: i32, start_offset: Option<u64>) -> DatagenSplit {
        DatagenSplit {
            split_index,
            split_num,
            start_offset,
        }
    }
}
