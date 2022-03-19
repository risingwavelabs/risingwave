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
//
use prost::Message;
use risingwave_pb::hummock::{HummockStaleSstables, HummockVersionRefId};

use crate::model::{MetadataModel, Transactional};

/// Column family name for stale hummock sstables.
/// `cf(hummock_stale_sstables)`: `HummockVersionRefId` -> `HummockStaleSstables`
const HUMMOCK_STALE_SSTABLES_CF_NAME: &str = "cf/hummock_stale_sstables";

/// `HummockStaleSstables` tracks `SSTables` no longer needed after the given version.
impl MetadataModel for HummockStaleSstables {
    type ProstType = HummockStaleSstables;
    type KeyType = HummockVersionRefId;

    fn cf_name() -> String {
        String::from(HUMMOCK_STALE_SSTABLES_CF_NAME)
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
        Ok(HummockVersionRefId {
            id: self.version_id,
        })
    }
}

impl Transactional for HummockStaleSstables {}
