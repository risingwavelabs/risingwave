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

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;

use itertools::Itertools;
use risingwave_hummock_sdk::compaction_group::{CompactionGroupId, Prefix};
use risingwave_hummock_sdk::key::{get_table_id, FullKey};
use risingwave_hummock_sdk::HummockSSTableId;
use risingwave_pb::common::VNodeBitmap;

use crate::hummock::sstable::restricted_builder::{ResourceLimiter, RestrictedBuilder};
use crate::hummock::sstable::sst_writer::SstWriter;
use crate::hummock::value::HummockValue;
use crate::hummock::{HummockResult, SSTableBuilder, Sstable};

pub type KeyValueGroupId = u64;
const DEFAULT_KEY_VALUE_GROUP_ID: KeyValueGroupId = KeyValueGroupId::MAX;

/// [`KeyValueGroupingImpl`] defines strategies to group key values
pub enum KeyValueGroupingImpl {
    VirtualNode(VirtualNodeGrouping),
    CompactionGroup(CompactionGroupGrouping),
}

trait KeyValueGrouping {
    fn group(
        &self,
        full_key: &FullKey<&[u8]>,
        value: &HummockValue<&[u8]>,
    ) -> Option<KeyValueGroupId>;
}

impl KeyValueGrouping for KeyValueGroupingImpl {
    fn group(
        &self,
        full_key: &FullKey<&[u8]>,
        value: &HummockValue<&[u8]>,
    ) -> Option<KeyValueGroupId> {
        match self {
            KeyValueGroupingImpl::VirtualNode(grouping) => grouping.group(full_key, value),
            KeyValueGroupingImpl::CompactionGroup(grouping) => grouping.group(full_key, value),
        }
    }
}

/// Groups key value by compaction group
pub struct CompactionGroupGrouping {
    prefixes: HashMap<Prefix, CompactionGroupId>,
}

impl CompactionGroupGrouping {
    pub fn new(prefixes: HashMap<Prefix, CompactionGroupId>) -> Self {
        Self { prefixes }
    }
}

impl KeyValueGrouping for CompactionGroupGrouping {
    fn group(
        &self,
        full_key: &FullKey<&[u8]>,
        _value: &HummockValue<&[u8]>,
    ) -> Option<KeyValueGroupId> {
        let prefix = get_table_id(full_key.inner()).unwrap();
        self.prefixes.get(&prefix.into()).cloned().map(|v| v.into())
    }
}

/// Groups key value by virtual node
pub struct VirtualNodeGrouping {
    vnode2unit: Arc<HashMap<u32, Vec<u32>>>,
}

impl VirtualNodeGrouping {
    pub fn new(vnode2unit: Arc<HashMap<u32, Vec<u32>>>) -> Self {
        VirtualNodeGrouping { vnode2unit }
    }
}

impl KeyValueGrouping for VirtualNodeGrouping {
    fn group(
        &self,
        full_key: &FullKey<&[u8]>,
        value: &HummockValue<&[u8]>,
    ) -> Option<KeyValueGroupId> {
        if let Some(table_id) = get_table_id(full_key.inner()) {
            self.vnode2unit.get(&table_id).map(|mapping| {
                mapping[match value {
                    HummockValue::Put(meta, _) => meta.vnode,
                    HummockValue::Delete(meta) => meta.vnode,
                } as usize] as KeyValueGroupId
            })
        } else {
            None
        }
    }
}

/// A wrapper builder which automatically split key-value pairs into
/// multiple `SSTables`, based on [`KeyValueGroupingImpl`].
pub struct GroupedSstableBuilder<B> {
    get_id_and_builder: B,
    grouping: KeyValueGroupingImpl,
    builders: HashMap<KeyValueGroupId, RestrictedBuilder<B>>,
    sst_writer: Arc<SstWriter>,
    limiter: Arc<ResourceLimiter>,
    resource_size: u64,
}

impl<B, F> GroupedSstableBuilder<B>
where
    B: Clone + Fn() -> F,
    F: Future<Output = HummockResult<(HummockSSTableId, SSTableBuilder)>>,
{
    pub fn new(
        get_id_and_builder: B,
        grouping: KeyValueGroupingImpl,
        sst_writer: Arc<SstWriter>,
        limiter: Arc<ResourceLimiter>,
        resource_size: u64,
    ) -> Self {
        let inner_builder = RestrictedBuilder::new(
            get_id_and_builder.clone(),
            sst_writer.clone(),
            limiter.clone(),
            resource_size,
        );
        Self {
            get_id_and_builder,
            grouping,
            builders: HashMap::from([(DEFAULT_KEY_VALUE_GROUP_ID, inner_builder)]),
            sst_writer,
            limiter,
            resource_size,
        }
    }

    pub fn len(&self) -> usize {
        self.builders.iter().map(|(_k, v)| v.len()).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.builders.iter().all(|(_k, v)| v.is_empty())
    }

    pub async fn add_full_key(
        &mut self,
        full_key: FullKey<&[u8]>,
        value: HummockValue<&[u8]>,
        allow_split: bool,
    ) -> HummockResult<()> {
        // TODO: maybe we want panic rather than DEFAULT_KEY_VALUE_GROUP_ID
        let group_id = self
            .grouping
            .group(&full_key, &value)
            .unwrap_or(DEFAULT_KEY_VALUE_GROUP_ID);
        let entry = self.builders.entry(group_id).or_insert_with(|| {
            RestrictedBuilder::new(
                self.get_id_and_builder.clone(),
                self.sst_writer.clone(),
                self.limiter.clone(),
                self.resource_size,
            )
        });
        entry.add_full_key(full_key, value, allow_split).await
    }

    pub fn seal_current(&mut self) {
        self.builders
            .iter_mut()
            .for_each(|(_k, v)| v.seal_current());
    }

    pub async fn finish(self) -> HummockResult<Vec<(Sstable, Vec<VNodeBitmap>)>> {
        let mut ssts = vec![];
        let mut vnode_bitmaps = vec![];
        for (_, builder) in self.builders {
            for (sst, vnode_bitmap) in builder.finish().await? {
                ssts.push(sst);
                vnode_bitmaps.push(vnode_bitmap);
            }
        }
        let result = ssts.into_iter().zip_eq(vnode_bitmaps).collect_vec();
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicU64;
    use std::sync::atomic::Ordering::SeqCst;

    use bytes::Buf;

    use super::*;
    use crate::hummock::iterator::test_utils::mock_sstable_store;
    use crate::hummock::sstable::utils::CompressionAlgorithm;
    use crate::hummock::{SSTableBuilderOptions, DEFAULT_RESTART_INTERVAL};

    #[tokio::test]
    async fn test_compaction_group_grouping() {
        let next_id = AtomicU64::new(1001);
        let block_size = 1 << 10;
        let table_capacity = 4 * block_size;
        let get_id_and_builder = || async {
            Ok((
                next_id.fetch_add(1, SeqCst),
                SSTableBuilder::new(SSTableBuilderOptions {
                    capacity: table_capacity,
                    block_capacity: block_size,
                    restart_interval: DEFAULT_RESTART_INTERVAL,
                    bloom_false_positive: 0.1,
                    compression_algorithm: CompressionAlgorithm::None,
                }),
            ))
        };
        let prefix = b"\x01\x02\x03\x04".as_slice().get_u32();
        // one compaction group defined
        let grouping = KeyValueGroupingImpl::CompactionGroup(CompactionGroupGrouping::new(
            HashMap::from([(prefix.into(), 1.into())]),
        ));
        let sstable_store = mock_sstable_store();
        let mut builder = GroupedSstableBuilder::new(
            get_id_and_builder,
            grouping,
            Arc::new(SstWriter::new(sstable_store)),
            Arc::new(ResourceLimiter::new(1024 * 1024 * 1024)),
            1024 * 1024 * 256,
        );
        for i in 0..10 {
            // key value belongs to no compaction group
            builder
                .add_full_key(
                    FullKey::from_user_key(
                        b"\x74\x00\x00\x00\x00".to_vec(),
                        (table_capacity - i) as u64,
                    )
                    .as_slice(),
                    HummockValue::put(b"value"),
                    false,
                )
                .await
                .unwrap();
            // key value belongs to compaction group `prefix`
            builder
                .add_full_key(
                    FullKey::from_user_key(
                        [b"\x74", prefix.to_be_bytes().as_slice()].concat().to_vec(),
                        (table_capacity - i) as u64,
                    )
                    .as_slice(),
                    HummockValue::put(b"value"),
                    false,
                )
                .await
                .unwrap();
        }
        builder.seal_current();
        let results = builder.finish().await.unwrap();
        assert_eq!(results.len(), 2);
    }
}
