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

use async_trait::async_trait;

use crate::source::cdc::{CdcProperties, CdcSplit};
use crate::source::SplitEnumerator;

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct CdcSplitEnumerator {
    source_id: u32,
}

#[async_trait]
impl SplitEnumerator for CdcSplitEnumerator {
    type Properties = CdcProperties;
    type Split = CdcSplit;

    async fn new(props: CdcProperties) -> anyhow::Result<CdcSplitEnumerator> {
        Ok(Self {
            source_id: props.source_id,
        })
    }

    async fn list_splits(&mut self) -> anyhow::Result<Vec<CdcSplit>> {
        // CDC source only supports single split
        let splits = vec![CdcSplit {
            source_id: self.source_id,
            start_offset: None,
        }];
        Ok(splits)
    }
}
