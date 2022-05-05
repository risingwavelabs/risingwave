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
use async_trait::async_trait;

use super::NEXMARK_CONFIG_SPLIT_NUM;
use crate::base::SplitEnumerator;
use crate::nexmark::split::NexmarkSplit;
use crate::utils::AnyhowProperties;

pub struct NexmarkSplitEnumerator {
    split_num: i32,
}

impl NexmarkSplitEnumerator {
    pub fn new(properties: &AnyhowProperties) -> anyhow::Result<NexmarkSplitEnumerator> {
        let split_num = properties
            .get_nexmark(NEXMARK_CONFIG_SPLIT_NUM)
            .unwrap_or_else(|_| "1".to_string())
            .parse::<i32>()
            .map_err(|e| anyhow!(e))?;

        Ok(Self { split_num })
    }
}

#[async_trait]
impl SplitEnumerator for NexmarkSplitEnumerator {
    type Split = NexmarkSplit;

    async fn list_splits(&mut self) -> anyhow::Result<Vec<NexmarkSplit>> {
        let mut splits = vec![];
        for i in 0..self.split_num {
            splits.push(NexmarkSplit {
                split_num: self.split_num,
                split_index: i,
                start_offset: None,
            });
        }
        Ok(splits)
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result;

    use super::*;
    use crate::SourceSplit;
    #[tokio::test]
    async fn test_nexmark_split_enumerator() -> Result<()> {
        let mut enumerator = NexmarkSplitEnumerator { split_num: 4 };
        let list_splits_resp = enumerator.list_splits().await?;
        // println!("{:#?}", list_splits_resp);
        assert_eq!(list_splits_resp.len(), 4);
        assert_eq!(list_splits_resp[0].id(), "4-0".to_string());
        assert_eq!(list_splits_resp[1].id(), "4-1".to_string());
        assert_eq!(list_splits_resp[2].id(), "4-2".to_string());
        assert_eq!(list_splits_resp[3].id(), "4-3".to_string());
        Ok(())
    }
}
