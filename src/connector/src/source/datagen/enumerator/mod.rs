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

use async_trait::async_trait;

use crate::source::datagen::{DatagenProperties, DatagenSplit};
use crate::source::SplitEnumerator;

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct DatagenSplitEnumerator {
    split_num: i32,
}

#[async_trait]
impl SplitEnumerator for DatagenSplitEnumerator {
    type Properties = DatagenProperties;
    type Split = DatagenSplit;

    async fn new(properties: DatagenProperties) -> anyhow::Result<DatagenSplitEnumerator> {
        let split_num = properties.split_num.unwrap_or_else(|| "1".to_string());
        let split_num = split_num.parse::<i32>()?;
        Ok(Self { split_num })
    }

    async fn list_splits(&mut self) -> anyhow::Result<Vec<DatagenSplit>> {
        let mut splits = vec![];
        for i in 0..self.split_num {
            splits.push(DatagenSplit {
                split_num: self.split_num,
                split_index: i,
                start_offset: None,
            });
        }
        Ok(splits)
    }
}
