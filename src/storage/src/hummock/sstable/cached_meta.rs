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

//! Cache-resident SST metadata and its existing read interface.
//! The private key index owns prefix encoding and lookup; format adapts existing Native/Foyer bytes.

use std::cmp::Ordering;
use std::ops::{Bound, Range};

use risingwave_common::catalog::TableId;
use risingwave_common::hash::VirtualNode;
use risingwave_hummock_sdk::key::FullKey;

use super::{BlockMeta, MonotonicDeleteEvent};
#[cfg(test)]
use super::{Sstable, SstableMeta};

mod format;
mod key_index;

pub(super) use format::DeserializedSstable;
use key_index::PrefixKeys;

/// The address and counters of a cached block. Keys belong to `CachedSstableMeta`'s prefix index.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CachedBlockMeta {
    pub offset: u32,
    pub len: u32,
    pub uncompressed_size: u32,
    pub total_key_count: u32,
    pub stale_key_count: u32,
    table_id: TableId,
}

impl CachedBlockMeta {
    pub fn table_id(&self) -> TableId {
        self.table_id
    }
}

impl From<&BlockMeta> for CachedBlockMeta {
    #[inline]
    fn from(block: &BlockMeta) -> Self {
        Self {
            offset: block.offset,
            len: block.len,
            uncompressed_size: block.uncompressed_size,
            total_key_count: block.total_key_count,
            stale_key_count: block.stale_key_count,
            table_id: block.table_id(),
        }
    }
}

/// Cache-resident metadata. Native SST encoding and writer metadata use `SstableMeta` instead.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CachedSstableMeta {
    pub block_metas: Vec<CachedBlockMeta>,
    keys: PrefixKeys,
    pub estimated_size: u32,
    pub key_count: u32,
    pub smallest_key: Vec<u8>,
    pub largest_key: Vec<u8>,
    pub meta_offset: u64,
    pub monotonic_tombstone_events: Vec<MonotonicDeleteEvent>,
    pub version: u32,
}

impl CachedSstableMeta {
    /// Restores one key into reader-owned scratch, without retaining decoded keys in the cache.
    pub fn block_smallest_key<'a>(&self, index: usize, key: &'a mut Vec<u8>) -> &'a [u8] {
        let block = &self.block_metas[index];
        self.keys.restore_key(index, block.table_id, key)
    }

    pub(in crate::hummock) fn block_smallest_vnode(&self, index: usize) -> Option<VirtualNode> {
        self.keys.vnode(index)
    }

    /// Compares a block boundary without allocating or restoring its full key.
    pub(in crate::hummock) fn compare_block_key(
        &self,
        index: usize,
        target: FullKey<&[u8]>,
    ) -> Ordering {
        self.keys
            .compare_key(index, self.block_metas[index].table_id, target)
    }

    /// Returns the absolute index after the keys within `bound` in the requested range.
    pub fn partition_point(&self, range: Range<usize>, bound: Bound<FullKey<&[u8]>>) -> usize {
        self.keys.partition_point(&self.block_metas, range, bound)
    }

    /// Reconstructs an owned `BlockMeta`, allocating its key, for raw copying or inspection.
    pub fn block_meta(&self, index: usize) -> BlockMeta {
        let block = &self.block_metas[index];
        let mut smallest_key = Vec::new();
        self.block_smallest_key(index, &mut smallest_key);
        BlockMeta {
            smallest_key,
            offset: block.offset,
            len: block.len,
            uncompressed_size: block.uncompressed_size,
            total_key_count: block.total_key_count,
            stale_key_count: block.stale_key_count,
        }
    }

    pub(super) fn estimated_heap_size(&self) -> usize {
        self.block_metas.capacity() * std::mem::size_of::<CachedBlockMeta>()
            + self.keys.estimated_heap_size()
            + self.smallest_key.capacity()
            + self.largest_key.capacity()
            + self.monotonic_tombstone_events.capacity()
                * std::mem::size_of::<MonotonicDeleteEvent>()
    }
}

#[cfg(test)]
mod tests {
    use bytes::BufMut;
    use foyer::Code;
    use risingwave_common::util::epoch::test_epoch;
    use risingwave_hummock_sdk::EpochWithGap;
    use risingwave_hummock_sdk::key::TableKey;
    use risingwave_hummock_sdk::key::range_delete_backward_compatibility_serde_struct::{
        PointRange, UserKey,
    };

    use super::super::{SerdeSstable, VERSION};
    use super::*;

    #[test]
    fn test_cached_meta_long_keys_and_partial_ranges() {
        // Three epochs per user key put equal user keys on both sides of restart 16.
        let blocks: Vec<_> = (0..80)
            .map(|index| {
                let local = index % 40;
                let mut key = vec![b'x'; 70_000];
                key[..2].copy_from_slice(&[0, 1]);
                key.extend_from_slice(&(local / 3_u32).to_be_bytes());
                BlockMeta {
                    smallest_key: FullKey::for_test(
                        TableId::new(1 + index / 40),
                        key,
                        test_epoch((100 - local % 3).into()),
                    )
                    .encode(),
                    offset: index * 100,
                    len: 100,
                    uncompressed_size: 200,
                    total_key_count: 3,
                    stale_key_count: 1,
                }
            })
            .collect();
        let old_key_bytes: usize = blocks.iter().map(|block| block.smallest_key.len()).sum();
        let mut tombstone_key = Vec::new();
        tombstone_key.put_u32(1);
        tombstone_key.put_u32(9);
        tombstone_key.extend_from_slice(b"old-range");

        #[expect(deprecated)]
        let raw = SstableMeta {
            block_metas: blocks.clone(),
            smallest_key: blocks.first().unwrap().smallest_key.clone(),
            largest_key: blocks.last().unwrap().smallest_key.clone(),
            estimated_size: 80_000,
            key_count: 240,
            meta_offset: 8_000,
            version: VERSION,
            monotonic_tombstone_events: vec![MonotonicDeleteEvent {
                event_key: PointRange {
                    left_user_key: UserKey::decode_length_prefixed(&mut &tombstone_key[..]),
                    is_exclude_left_key: true,
                },
                new_epoch: test_epoch(90),
            }],
            ..Default::default()
        };
        let expected = Sstable::new(1.into(), raw.clone(), false);
        let decoded = Sstable::from_meta_bytes(1.into(), &raw.encode_to_bytes(), false).unwrap();
        assert_eq!(decoded.meta, expected.meta);
        let wire = bincode::serialize(&SerdeSstable {
            id: 1.into(),
            meta: raw,
        })
        .unwrap();
        let restored = Box::<Sstable>::decode(&mut &wire[..]).unwrap();
        assert_eq!(restored.meta, expected.meta);
        // Restoring an empty legacy filter produces a canonical XOR16 header on encoding.
        assert_eq!(
            expected.serde_encoded_size(),
            bincode::serialize(&expected).unwrap().len()
        );
        let meta = decoded.meta;
        assert!(meta.estimated_heap_size() < old_key_bytes / 8);
        for (index, original) in blocks.iter().enumerate() {
            assert_eq!(meta.block_meta(index), *original);
            let target = FullKey::decode(&original.smallest_key);
            for range in [0..80, 5..21, 15..17, 17..64, 33..78, 80..80] {
                let expected = range.start
                    + blocks[range.clone()]
                        .partition_point(|block| FullKey::decode(&block.smallest_key) <= target);
                assert_eq!(
                    meta.partition_point(range.clone(), Bound::Included(target)),
                    expected
                );
                for inclusive in [true, false] {
                    let before_end = |key: FullKey<&[u8]>| {
                        key.user_key < target.user_key
                            || (inclusive && key.user_key == target.user_key)
                    };
                    let expected = range.start
                        + blocks[range.clone()].partition_point(|block| {
                            before_end(FullKey::decode(&block.smallest_key))
                        });
                    assert_eq!(
                        meta.partition_point(
                            range.clone(),
                            if inclusive {
                                Bound::Included(FullKey {
                                    user_key: target.user_key,
                                    epoch_with_gap: EpochWithGap::new_min_epoch(),
                                })
                            } else {
                                Bound::Excluded(FullKey {
                                    user_key: target.user_key,
                                    epoch_with_gap: EpochWithGap::new_max_epoch(),
                                })
                            },
                        ),
                        expected
                    );
                }
            }
        }
        // The first group's restart belongs to another table; evaluate only the requested range.
        assert_eq!(
            meta.partition_point(
                40..79,
                Bound::Included(FullKey::decode(&blocks[39].smallest_key)),
            ),
            40
        );
    }

    #[test]
    fn test_cached_meta_bound_search_with_variable_user_keys() {
        // The final version of "a" starts its epoch with 'b', so the encoded prefix shared
        // with "ab" includes epoch bytes. Those bytes are not reusable user-key matches.
        let epochs = [u64::MAX, 0x6200_0000_0000_0001, 0x6200_0000_0000_0000];
        let users: &[&[u8]] = &[b"", b"a", b"ab", b"abc", b"b", b"x"];
        let mut blocks = Vec::new();
        for table in 1..=3 {
            for &user in users {
                for epoch in epochs.into_iter().chain(user.is_empty().then_some(0)) {
                    blocks.push(BlockMeta {
                        smallest_key: FullKey::new_with_gap_epoch(
                            TableId::new(table),
                            TableKey(user),
                            EpochWithGap::from_u64(epoch),
                        )
                        .encode(),
                        ..Default::default()
                    });
                }
            }
        }
        // Growing prefix keys force a fresh suffix comparison after an equal/shorter key,
        // across complete restart groups. Epoch bytes must never enter the shared prefix.
        for len in 1..=33 {
            blocks.push(BlockMeta {
                smallest_key: FullKey::new_with_gap_epoch(
                    TableId::new(4),
                    TableKey(vec![b'a'; len]),
                    EpochWithGap::from_u64(0x6161_6161_6161_6161),
                )
                .encode(),
                ..Default::default()
            });
        }
        let meta = CachedSstableMeta::from(SstableMeta {
            block_metas: blocks.clone(),
            ..Default::default()
        });
        let count = blocks.len();
        for (index, block) in blocks.iter().enumerate() {
            let target = FullKey::decode(&block.smallest_key);
            for (other_index, other) in blocks.iter().enumerate() {
                assert_eq!(
                    meta.compare_block_key(other_index, target),
                    FullKey::decode(&other.smallest_key).cmp(&target),
                    "boundary {other_index}, target {index}"
                );
            }
            assert_eq!(
                meta.partition_point(0..count, Bound::Included(target)),
                index + 1
            );
            assert_eq!(
                meta.partition_point(0..count, Bound::Excluded(target)),
                index
            );
        }
        let ranges = [
            0..count,
            1..count - 1,
            15..17,
            17..count - 2,
            19..35,
            31..33,
            32..33,
            0..1,
            count..count,
        ];
        for range in &ranges {
            assert_eq!(
                meta.partition_point(range.clone(), Bound::Unbounded),
                range.end
            );
            for table in 0..=4 {
                for user in [b"".as_slice(), b"a", b"aa", b"ab", b"abb", b"abcd", b"z"] {
                    for epoch in [0, 1, epochs[1], epochs[2], u64::MAX] {
                        let target = FullKey::new_with_gap_epoch(
                            TableId::new(table),
                            TableKey(user),
                            EpochWithGap::from_u64(epoch),
                        );
                        for inclusive in [true, false] {
                            let expected = range.start
                                + blocks[range.clone()].partition_point(|block| {
                                    let key = FullKey::decode(&block.smallest_key);
                                    if inclusive {
                                        key <= target
                                    } else {
                                        key < target
                                    }
                                });
                            assert_eq!(
                                meta.partition_point(
                                    range.clone(),
                                    if inclusive {
                                        Bound::Included(target)
                                    } else {
                                        Bound::Excluded(target)
                                    },
                                ),
                                expected,
                                "full bound: {range:?}, {target:?}, inclusive={inclusive}"
                            );
                        }
                    }
                    let target = FullKey::new_with_gap_epoch(
                        TableId::new(table),
                        TableKey(user),
                        EpochWithGap::new_min_epoch(),
                    );
                    for inclusive in [true, false] {
                        let expected = range.start
                            + blocks[range.clone()].partition_point(|block| {
                                let key = FullKey::decode(&block.smallest_key).user_key;
                                if inclusive {
                                    key <= target.user_key
                                } else {
                                    key < target.user_key
                                }
                            });
                        assert_eq!(
                            meta.partition_point(
                                range.clone(),
                                if inclusive {
                                    Bound::Included(target)
                                } else {
                                    Bound::Excluded(FullKey {
                                        epoch_with_gap: EpochWithGap::new_max_epoch(),
                                        ..target
                                    })
                                },
                            ),
                            expected,
                            "user bound: {range:?}, {target:?}, inclusive={inclusive}"
                        );
                    }
                }
            }
        }
    }
}
