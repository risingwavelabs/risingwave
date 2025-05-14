// Copyright 2025 RisingWave Labs
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

use std::collections::{HashMap, HashSet};

use risingwave_common::catalog::TableId;
use risingwave_pb::hummock::vector_index::PbVariant;
use risingwave_pb::hummock::vector_index_delta::vector_index_init::Config;
use risingwave_pb::hummock::vector_index_delta::{
    PbVectorIndexAdd, PbVectorIndexInit, vector_index_add,
};
use risingwave_pb::hummock::{
    PbFlatIndex, PbFlatIndexAdd, PbFlatIndexConfig, PbVectorIndex, PbVectorIndexDelta,
    vector_index_delta,
};

fn init_flat_index(config: PbFlatIndexConfig) -> PbFlatIndex {
    PbFlatIndex {
        config: Some(config),
        vector_files: vec![],
    }
}

fn apply_flat_index_add(flat_index: &mut PbFlatIndex, add: PbFlatIndexAdd) {
    flat_index.vector_files.extend(add.added_vector_files);
}

fn init_vector_index(init: PbVectorIndexInit) -> PbVectorIndex {
    let variant = match init.config.unwrap() {
        Config::Flat(config) => PbVariant::Flat(init_flat_index(config)),
    };
    PbVectorIndex {
        dimension: init.dimension,
        distance_type: init.distance_type,
        variant: Some(variant),
    }
}

fn apply_vector_index_add(variant: &mut PbVariant, add: PbVectorIndexAdd) {
    match variant {
        PbVariant::Flat(flat_index) => {
            let Some(vector_index_add::Add::Flat(add)) = add.add else {
                panic!("expect FlatIndexAdd but got {:?}", flat_index);
            };
            apply_flat_index_add(flat_index, add);
        }
    }
}

pub fn apply_vector_index_delta(
    vector_index: &mut HashMap<TableId, PbVectorIndex>,
    vector_index_delta: HashMap<TableId, PbVectorIndexDelta>,
    removed_table_ids: &HashSet<TableId>,
) {
    for (table_id, vector_index_delta) in vector_index_delta {
        match vector_index_delta.delta.unwrap() {
            vector_index_delta::Delta::Init(init) => {
                vector_index
                    .try_insert(table_id, init_vector_index(init))
                    .unwrap();
            }
            vector_index_delta::Delta::Adds(adds) => {
                let variant = vector_index
                    .get_mut(&table_id)
                    .unwrap()
                    .variant
                    .as_mut()
                    .unwrap();
                for add in adds.adds {
                    apply_vector_index_add(variant, add);
                }
            }
        }
    }

    // Remove the vector index for the tables that are removed
    vector_index.retain(|table_id, _| !removed_table_ids.contains(table_id));
}
