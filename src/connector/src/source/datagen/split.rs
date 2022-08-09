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

use anyhow::anyhow;
use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::source::base::SplitMetaData;
use crate::source::SplitId;

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

    fn encode_to_bytes(&self) -> Bytes {
        Bytes::from(serde_json::to_string(self).unwrap())
    }

    fn restore_from_bytes(bytes: &[u8]) -> anyhow::Result<Self> {
        serde_json::from_slice(bytes).map_err(|e| anyhow!(e))
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

    pub fn copy_with_offset(&self, start_offset: String) -> Self {
        Self::new(
            self.split_index,
            self.split_num,
            Some(start_offset.as_str().parse::<u64>().unwrap()),
        )
    }
}
