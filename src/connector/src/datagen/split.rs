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

use crate::base::SplitMetaData;

#[derive(Clone, Serialize, Deserialize, Debug, Default)]
pub struct DatagenSplit {}

impl SplitMetaData for DatagenSplit {
    fn id(&self) -> String {
        format!("{}", 0)
    }

    fn encode_to_bytes(&self) -> Bytes {
        Bytes::from(serde_json::to_string(self).unwrap())
    }

    fn restore_from_bytes(bytes: &[u8]) -> anyhow::Result<Self> {
        serde_json::from_slice(bytes).map_err(|e| anyhow!(e))
    }
}

impl DatagenSplit {
    pub fn new() -> Self {
        Self {}
    }

    pub fn update(&self, start_offset: String) -> Self {
        // TODO make sure fail over for generators
        log::warn!(
            "DatagenSplit is updated to {:?} but not applied",
            start_offset
        );
        Self {}
    }
}
