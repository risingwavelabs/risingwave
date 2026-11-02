// Copyright 2026 RisingWave Labs
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

use risingwave_common::catalog::TableId;
use risingwave_common::hash::VirtualNode;
use risingwave_common::util::epoch::test_epoch;
use risingwave_hummock_sdk::key::FullKey;
use risingwave_storage::compaction_catalog_manager::CompactionCatalogAgent;
use risingwave_storage::hummock::sstable::{
    FilterBuilder, InMemWriter, SstableBuilder, SstableBuilderOptions, SstableMeta,
};
use risingwave_storage::hummock::value::HummockValue;

// Build and native-decode an SST before any timed iterations. Shared-prefix keys keep
// the fixture bounded while exercising actual block metadata and filter builders.
pub async fn sstable_meta<F: FilterBuilder>(key_count: usize, key_len: usize) -> SstableMeta {
    let options = SstableBuilderOptions {
        capacity: 128 << 20,
        block_capacity: 64 << 10,
        estimated_output_key_count: Some(key_count),
        ..Default::default()
    };
    let mut builder = SstableBuilder::new(
        1_u64,
        InMemWriter::from(&options),
        F::create(options.filter_builder_options()),
        options,
        CompactionCatalogAgent::for_test(vec![TableId::default()]),
        None,
    );
    for index in 0..key_count {
        let mut key = VirtualNode::ZERO.to_be_bytes().to_vec();
        key.resize(key_len - 8, b'k');
        key.extend_from_slice(&(index as u64).to_be_bytes());
        let key = FullKey::for_test(TableId::default(), key, test_epoch(1));
        builder
            .add(key.to_ref(), HummockValue::put(&[42; 16]))
            .await
            .unwrap();
    }
    let (bytes, meta) = builder.finish().await.unwrap().writer_output;
    SstableMeta::decode(&bytes[meta.meta_offset as usize..]).unwrap()
}
