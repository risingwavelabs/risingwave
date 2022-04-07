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

use risingwave_common::error::Result;
use risingwave_hummock_sdk::{HummockVersionId, FIRST_VERSION_ID};
use risingwave_pb::hummock::HummockVersionRefId;

use crate::hummock::model::HUMMOCK_DEFAULT_CF_NAME;
use crate::model::MetadataModel;
use crate::storage::MetaStore;

/// Hummock current version id key.
/// `cf(hummock_default)`: `hummock_version_id_key` -> `HummockVersionRefId`
const HUMMOCK_VERSION_ID_KEY: &str = "current_version_id";

/// `CurrentHummockVersionId` tracks the current version id.
#[derive(Clone, Debug, PartialEq)]
pub struct CurrentHummockVersionId {
    id: HummockVersionId,
}

impl MetadataModel for CurrentHummockVersionId {
    type ProstType = HummockVersionRefId;
    type KeyType = String;

    fn cf_name() -> String {
        HUMMOCK_DEFAULT_CF_NAME.to_string()
    }

    fn to_protobuf(&self) -> Self::ProstType {
        HummockVersionRefId { id: self.id }
    }

    fn from_protobuf(prost: Self::ProstType) -> Self {
        Self { id: prost.id }
    }

    fn key(&self) -> Result<Self::KeyType> {
        Ok(HUMMOCK_VERSION_ID_KEY.to_string())
    }
}

impl CurrentHummockVersionId {
    pub fn new() -> CurrentHummockVersionId {
        CurrentHummockVersionId {
            id: FIRST_VERSION_ID,
        }
    }

    pub async fn get<S: MetaStore>(meta_store: &S) -> Result<Option<CurrentHummockVersionId>> {
        CurrentHummockVersionId::select(meta_store, &HUMMOCK_VERSION_ID_KEY.to_string()).await
    }

    /// Increase version id, return previous one
    pub fn increase(&mut self) -> HummockVersionId {
        let previous_id = self.id;
        self.id += 1;
        previous_id
    }

    pub fn id(&self) -> HummockVersionId {
        self.id
    }
}
