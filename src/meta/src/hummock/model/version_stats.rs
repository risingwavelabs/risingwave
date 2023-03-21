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

use prost::Message;
use risingwave_hummock_sdk::HummockVersionId;
use risingwave_pb::hummock::HummockVersionStats;

use crate::hummock::model::HUMMOCK_VERSION_STATS_CF_NAME;
use crate::model::{MetadataModel, MetadataModelResult};

/// `HummockVersionStats` stores stats for hummock version.
/// Currently it only persists one row for latest version.
impl MetadataModel for HummockVersionStats {
    type KeyType = HummockVersionId;
    type PbType = HummockVersionStats;

    fn cf_name() -> String {
        String::from(HUMMOCK_VERSION_STATS_CF_NAME)
    }

    fn to_protobuf(&self) -> Self::PbType {
        self.clone()
    }

    fn to_protobuf_encoded_vec(&self) -> Vec<u8> {
        self.encode_to_vec()
    }

    fn from_protobuf(prost: Self::PbType) -> Self {
        prost
    }

    fn key(&self) -> MetadataModelResult<Self::KeyType> {
        Ok(0)
    }
}
