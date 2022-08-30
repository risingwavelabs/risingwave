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

pub mod manager;

use std::borrow::Borrow;
use std::collections::{HashMap, HashSet};

use itertools::Itertools;
pub use risingwave_common::catalog::TableOption;
use risingwave_hummock_sdk::compaction_group::StateTableId;
use risingwave_hummock_sdk::CompactionGroupId;
use risingwave_pb::hummock::CompactionConfig;

use crate::model::{MetadataModel, MetadataModelResult};

#[derive(Debug, Clone, PartialEq)]
pub struct CompactionGroup {
    group_id: CompactionGroupId,
    member_table_ids: HashSet<StateTableId>,
    compaction_config: CompactionConfig,
    table_id_to_options: HashMap<StateTableId, TableOption>,
}

impl CompactionGroup {
    pub fn new(group_id: CompactionGroupId, compaction_config: CompactionConfig) -> Self {
        Self {
            group_id,
            member_table_ids: Default::default(),
            compaction_config,
            table_id_to_options: HashMap::default(),
        }
    }

    pub fn group_id(&self) -> CompactionGroupId {
        self.group_id
    }

    pub fn member_table_ids(&self) -> &HashSet<StateTableId> {
        &self.member_table_ids
    }

    pub fn compaction_config(&self) -> CompactionConfig {
        self.compaction_config.clone()
    }

    pub fn table_id_to_options(&self) -> &HashMap<u32, TableOption> {
        &self.table_id_to_options
    }

    pub fn set_compaction_config(&mut self, compaction_config: CompactionConfig) {
        // These configs cannot be modified since created.
        assert_eq!(
            self.compaction_config.max_level,
            compaction_config.max_level,
        );
        assert_eq!(
            self.compaction_config.compaction_mode,
            compaction_config.compaction_mode,
        );
        assert_eq!(
            self.compaction_config.compression_algorithm,
            compaction_config.compression_algorithm,
        );
        self.compaction_config = compaction_config;
    }
}

impl From<&risingwave_pb::hummock::CompactionGroup> for CompactionGroup {
    fn from(compaction_group: &risingwave_pb::hummock::CompactionGroup) -> Self {
        Self {
            group_id: compaction_group.id,
            member_table_ids: compaction_group.member_table_ids.iter().cloned().collect(),
            compaction_config: compaction_group
                .compaction_config
                .as_ref()
                .cloned()
                .unwrap(),
            table_id_to_options: compaction_group
                .table_id_to_options
                .iter()
                .map(|id_to_table_option| (*id_to_table_option.0, id_to_table_option.1.into()))
                .collect::<HashMap<_, _>>(),
        }
    }
}

impl From<&CompactionGroup> for risingwave_pb::hummock::CompactionGroup {
    fn from(compaction_group: &CompactionGroup) -> Self {
        Self {
            id: compaction_group.group_id,
            member_table_ids: compaction_group
                .member_table_ids
                .iter()
                .cloned()
                .collect_vec(),
            compaction_config: Some(compaction_group.compaction_config.clone()),
            table_id_to_options: compaction_group
                .table_id_to_options
                .iter()
                .map(|id_to_table_option| (*id_to_table_option.0, id_to_table_option.1.into()))
                .collect::<HashMap<_, _>>(),
        }
    }
}

const HUMMOCK_COMPACTION_GROUP_CF_NAME: &str = "cf/hummock_compaction_group";

impl MetadataModel for CompactionGroup {
    type KeyType = CompactionGroupId;
    type ProstType = risingwave_pb::hummock::CompactionGroup;

    fn cf_name() -> String {
        String::from(HUMMOCK_COMPACTION_GROUP_CF_NAME)
    }

    fn to_protobuf(&self) -> Self::ProstType {
        self.borrow().into()
    }

    fn from_protobuf(prost: Self::ProstType) -> Self {
        prost.borrow().into()
    }

    fn key(&self) -> MetadataModelResult<Self::KeyType> {
        Ok(self.group_id)
    }
}
