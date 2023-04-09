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

use std::borrow::Borrow;
use std::sync::Arc;

pub use risingwave_common::catalog::TableOption;
use risingwave_hummock_sdk::CompactionGroupId;
use risingwave_pb::hummock::CompactionConfig;

use crate::hummock::model::HUMMOCK_COMPACTION_GROUP_CONFIG_CF_NAME;
use crate::model::{MetadataModel, MetadataModelResult};

#[derive(Debug, Clone, PartialEq)]
pub struct CompactionGroup {
    pub(crate) group_id: CompactionGroupId,
    pub(crate) compaction_config: Arc<CompactionConfig>,
}

impl CompactionGroup {
    pub fn new(group_id: CompactionGroupId, compaction_config: CompactionConfig) -> Self {
        Self {
            group_id,
            compaction_config: Arc::new(compaction_config),
        }
    }

    pub fn group_id(&self) -> CompactionGroupId {
        self.group_id
    }

    pub fn compaction_config(&self) -> Arc<CompactionConfig> {
        self.compaction_config.clone()
    }
}

impl From<&risingwave_pb::hummock::CompactionGroup> for CompactionGroup {
    fn from(compaction_group: &risingwave_pb::hummock::CompactionGroup) -> Self {
        Self {
            group_id: compaction_group.id,
            compaction_config: Arc::new(
                compaction_group
                    .compaction_config
                    .as_ref()
                    .cloned()
                    .unwrap(),
            ),
        }
    }
}

impl From<&CompactionGroup> for risingwave_pb::hummock::CompactionGroup {
    fn from(compaction_group: &CompactionGroup) -> Self {
        Self {
            id: compaction_group.group_id,
            compaction_config: Some(compaction_group.compaction_config.as_ref().clone()),
        }
    }
}

impl MetadataModel for CompactionGroup {
    type KeyType = CompactionGroupId;
    type PbType = risingwave_pb::hummock::CompactionGroup;

    fn cf_name() -> String {
        String::from(HUMMOCK_COMPACTION_GROUP_CONFIG_CF_NAME)
    }

    fn to_protobuf(&self) -> Self::PbType {
        self.borrow().into()
    }

    fn from_protobuf(prost: Self::PbType) -> Self {
        prost.borrow().into()
    }

    fn key(&self) -> MetadataModelResult<Self::KeyType> {
        Ok(self.group_id)
    }
}
