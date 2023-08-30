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

use anyhow::anyhow;
use async_trait::async_trait;

use super::source::NatsSplit;
use super::NatsProperties;
use crate::source::{SplitEnumerator, SourceEnumeratorContextRef};

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct NatsSplitEnumerator {
    source_id: u32,
    split_num: i32,
}

#[async_trait]
impl SplitEnumerator for NatsSplitEnumerator {
    type Properties = NatsProperties;
    type Split = NatsSplit;

    async fn new(
        properties: Self::Properties,
        context: SourceEnumeratorContextRef,
    ) -> anyhow::Result<NatsSplitEnumerator> {
        Ok(Self{
            source_id: 0,
            split_num: 0,
        })
    }

    async fn list_splits(&mut self) -> anyhow::Result<Vec<NatsSplit>> {
        let mut splits = vec![];
        for i in 0..self.split_num {
            splits.push(NatsSplit {
                // split_num: self.split_num,
                // split_index: i,
                // start_offset: None,
                partition : 0,
            });
        }
        Ok(splits)
    }
}
