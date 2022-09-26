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

use risingwave_pb::hummock::GroupVersionRefId;

use crate::model::{MetadataModel, MetadataModelResult};
use crate::storage::MetaStore;

/// current group manager version id key.
/// `cf(hummock_default)`: `group_version_id_key` -> `GroupVersionRefId`
const GROUP_VERSION_ID_KEY: &str = "current_group_version_id";

/// `CurrentGroupVersionId` tracks the current version id.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct CurrentGroupVersionId {
    version: u64,
}

impl MetadataModel for CurrentGroupVersionId {
    type KeyType = String;
    type ProstType = GroupVersionRefId;

    fn cf_name() -> String {
        "hummock_default_cf".to_string()
    }

    fn to_protobuf(&self) -> Self::ProstType {
        GroupVersionRefId {
            version: self.version,
        }
    }

    fn from_protobuf(prost: Self::ProstType) -> Self {
        Self {
            version: prost.version,
        }
    }

    fn key(&self) -> MetadataModelResult<Self::KeyType> {
        Ok(GROUP_VERSION_ID_KEY.to_string())
    }
}

impl CurrentGroupVersionId {
    pub async fn get<S: MetaStore>(
        meta_store: &S,
    ) -> MetadataModelResult<Option<CurrentGroupVersionId>> {
        CurrentGroupVersionId::select(meta_store, &GROUP_VERSION_ID_KEY.to_string()).await
    }

    /// Increase version, return previous one
    pub fn increase(&mut self) -> u64 {
        let previous_version = self.version;
        self.version += 1;
        previous_version
    }

    pub fn version(&self) -> u64 {
        self.version
    }
}
