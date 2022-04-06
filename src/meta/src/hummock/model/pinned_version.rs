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

use prost::Message;
use risingwave_common::storage::HummockVersionId;
use risingwave_pb::hummock::{HummockContextRefId, HummockPinnedVersion};

use crate::model::MetadataModel;

/// Column family name for hummock pinned version
/// `cf(hummock_pinned_version)`: `HummockContextRefId` -> `HummockPinnedVersion`
const HUMMOCK_PINNED_VERSION_CF_NAME: &str = "cf/hummock_pinned_version";

/// `HummockPinnedVersion` tracks pinned versions by given context id.
impl MetadataModel for HummockPinnedVersion {
    type ProstType = HummockPinnedVersion;
    type KeyType = HummockContextRefId;

    fn cf_name() -> String {
        String::from(HUMMOCK_PINNED_VERSION_CF_NAME)
    }

    fn to_protobuf(&self) -> Self::ProstType {
        self.clone()
    }

    fn to_protobuf_encoded_vec(&self) -> Vec<u8> {
        self.encode_to_vec()
    }

    fn from_protobuf(prost: Self::ProstType) -> Self {
        prost
    }

    fn key(&self) -> risingwave_common::error::Result<Self::KeyType> {
        Ok(HummockContextRefId {
            id: self.context_id,
        })
    }
}

pub trait HummockPinnedVersionExt {
    fn pin_version(&mut self, version_id: HummockVersionId);
    fn unpin_version(&mut self, version_id: HummockVersionId);
}

impl HummockPinnedVersionExt for HummockPinnedVersion {
    fn pin_version(&mut self, version_id: HummockVersionId) {
        let found = self.version_id.iter().position(|&v| v == version_id);
        if found.is_none() {
            self.version_id.push(version_id);
        }
    }

    fn unpin_version(&mut self, pinned_version_id: HummockVersionId) {
        let found = self.version_id.iter().position(|&v| v == pinned_version_id);
        if let Some(pos) = found {
            self.version_id.remove(pos);
        }
    }
}
