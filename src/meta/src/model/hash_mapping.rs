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

use risingwave_common::util::compress::{compress_data, decompress_data};
use risingwave_pb::common::ParallelUnitMapping;

use super::MetadataModel;
use crate::cluster::ParallelUnitId;
use crate::manager::TableId;

/// Column family name for hash mapping.
const HASH_MAPPING_CF_NAME: &str = "cf/hash_mapping";

/// `ParallelUnitMapping` stores the hash mapping from `VirtualNode` to `ParallelUnitId` based
/// on consistent hash.
impl MetadataModel for ParallelUnitMapping {
    type KeyType = u32;
    type ProstType = ParallelUnitMapping;

    fn cf_name() -> String {
        HASH_MAPPING_CF_NAME.to_string()
    }

    fn to_protobuf(&self) -> Self::ProstType {
        self.clone()
    }

    fn from_protobuf(prost: Self::ProstType) -> Self {
        prost
    }

    fn key(&self) -> risingwave_common::error::Result<Self::KeyType> {
        Ok(self.table_id)
    }
}

pub fn original_hash_mapping(compressed_mapping: &ParallelUnitMapping) -> Vec<ParallelUnitId> {
    decompress_data(
        &compressed_mapping.original_indices,
        &compressed_mapping.data,
    )
}

pub fn compressed_hash_mapping(
    table_id: TableId,
    hash_mapping: &[ParallelUnitId],
) -> ParallelUnitMapping {
    let (original_indices, compressed_data) = compress_data(hash_mapping);
    ParallelUnitMapping {
        table_id,
        original_indices,
        data: compressed_data,
    }
}
