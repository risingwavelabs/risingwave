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

#![cfg(feature = "test")]

use bytes::Bytes;
use risingwave_common::catalog::TableId;
use risingwave_common::hash::VirtualNode;
use risingwave_common::util::epoch::test_epoch;
use risingwave_hummock_sdk::EpochWithGap;
use risingwave_hummock_sdk::key::{FullKey, TableKey};
use risingwave_storage::compaction_catalog_manager::CompactionCatalogAgent;
use risingwave_storage::hummock::sstable::{
    Block, BlockBuilder, BlockBuilderOptions, BlockedXor8FilterBuilder, BlockedXor16FilterBuilder,
    CompressionAlgorithm, FilterBuilder, InMemWriter, SstableBuilder, SstableBuilderOptions,
    Xor8FilterBuilder, Xor16FilterBuilder, XorFilterReader, xxhash64_checksum,
};
use risingwave_storage::hummock::value::HummockValue;

// Frozen against the pre-optimization writer. Updating these fixtures requires an explicit
// encoding review, not just a successful round trip through the current decoder.
// An optional export also allows byte-for-byte comparison of two independently built binaries.
fn fingerprint(name: String, data: &[u8]) -> (String, usize, u64) {
    if let Ok(dir) = std::env::var("RW_BUILDER_ORACLE_DIR") {
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(std::path::Path::new(&dir).join(&name), data).unwrap();
    }
    (name, data.len(), xxhash64_checksum(data))
}

#[test]
fn block_encoding_compatibility() {
    let mut fingerprints = Vec::new();
    for compression in [
        CompressionAlgorithm::None,
        CompressionAlgorithm::Lz4,
        CompressionAlgorithm::Zstd,
    ] {
        let mut builder = BlockBuilder::new(BlockBuilderOptions {
            capacity: 2 * 1024 * 1024,
            restart_interval: 16,
            compression_algorithm: compression,
        });
        // Crossing the encoded-key length types, long shared prefixes capped at MAX_KEY_LEN,
        // shrinking keys, repeated epochs, scheduled restarts, and builder reuse.
        for round in 0..2 {
            for (group, key_len) in [32, 255, 256, 65535, 65536, 32].into_iter().enumerate() {
                for i in 0u64..18 {
                    let mut key = vec![b'a' + group as u8; key_len - 8];
                    key[key_len - 16..].copy_from_slice(&i.to_be_bytes());
                    for (epoch, spill_offset) in [(2, 2u16), (2, 1), (1, 0)] {
                        let mut encoded = key.clone();
                        encoded.extend_from_slice(
                            &(test_epoch(epoch) + u64::from(spill_offset)).to_be_bytes(),
                        );
                        let value = vec![42; if i == 8 { 256 } else { 255 }];
                        builder.add(TableId::default(), &encoded, &value);
                    }
                }
            }
            let size = builder.uncompressed_block_size();
            let data = builder.build();
            fingerprints.push(fingerprint(format!("block_{compression:?}_{round}"), data));
            Block::decode(Bytes::copy_from_slice(data), size).unwrap();
            builder.clear();
        }
    }
    expect_test::expect![[r#"
        [
            (
                "block_None_0",
                810206,
                7063971759611180377,
            ),
            (
                "block_None_1",
                810206,
                7063971759611180377,
            ),
            (
                "block_Lz4_0",
                4961,
                15536231950184383243,
            ),
            (
                "block_Lz4_1",
                4961,
                15536231950184383243,
            ),
            (
                "block_Zstd_0",
                1287,
                1683587681788505959,
            ),
            (
                "block_Zstd_1",
                1287,
                1683587681788505959,
            ),
        ]
    "#]]
    .assert_debug_eq(&fingerprints);
}

async fn sstable_fixture<F: FilterBuilder>(
    compression: CompressionAlgorithm,
    single_table: bool,
) -> Vec<u8> {
    let options = SstableBuilderOptions {
        capacity: 256 * 1024,
        block_capacity: 64 * 1024,
        compression_algorithm: compression,
        // The existing shortening helper requires both keys to belong to the same table.
        shorten_block_meta_key_threshold: single_table.then_some(16),
        max_vnode_key_range_bytes: single_table.then_some(1024 * 1024),
        ..Default::default()
    };
    let mut builder = SstableBuilder::new(
        1,
        InMemWriter::from(&options),
        F::create(options.filter_builder_options()),
        options,
        CompactionCatalogAgent::for_test(vec![0, 1]),
        None,
    );
    let mut boundary_checks = Vec::new();
    for table in 0..if single_table { 1 } else { 2 } {
        for vnode in 0..2 {
            for (group, key_len) in [24, 247, 248, 65527, 65528].into_iter().enumerate() {
                for i in 0u64..18 {
                    let mut key = VirtualNode::from_index(vnode).to_be_bytes().to_vec();
                    key.push(group as u8);
                    key.resize(key_len - 8, b'k');
                    key.extend_from_slice(&i.to_be_bytes());
                    let value_len = match i {
                        0 => None,
                        1 => Some(0),
                        2 => Some(254),
                        3 => Some(255),
                        13 => Some(65534),
                        14 => Some(65535),
                        _ => Some(8),
                    };
                    let payload = vec![42; value_len.unwrap_or(0)];
                    let value =
                        value_len.map_or(HummockValue::Delete, |_| HummockValue::put(&payload[..]));
                    for (epoch, spill_offset) in [(2, 2u16), (2, 1), (1, 0)] {
                        builder
                            .add(
                                FullKey::new_with_gap_epoch(
                                    TableId::new(table),
                                    TableKey(&key[..]),
                                    EpochWithGap::new(test_epoch(epoch), spill_offset),
                                ),
                                value,
                            )
                            .await
                            .unwrap();
                        boundary_checks.push(u8::from(builder.reach_capacity()));
                        boundary_checks.extend_from_slice(
                            &(builder.current_block_size() as u64).to_le_bytes(),
                        );
                    }
                }
            }
        }
    }
    let output = builder.finish().await.unwrap();
    let (data, meta) = output.writer_output;
    // Include metadata outside the SST too (vnode ranges, key ranges, counts and epoch bounds),
    // but exclude LocalSstableInfo.created_at, which intentionally uses wall-clock time.
    let mut fixture = data.to_vec();
    fixture.extend_from_slice(format!("{:?}", output.sst_info.sst_info).as_bytes());
    fixture.extend_from_slice(&boundary_checks);
    for block in &meta.block_metas {
        Block::decode(
            data.slice(block.offset as usize..(block.offset + block.len) as usize),
            block.uncompressed_size as usize,
        )
        .unwrap();
    }
    fixture
}

#[tokio::test]
async fn sstable_encoding_compatibility() {
    let mut fingerprints = Vec::new();
    for compression in [
        CompressionAlgorithm::None,
        CompressionAlgorithm::Lz4,
        CompressionAlgorithm::Zstd,
    ] {
        for single_table in [true, false] {
            macro_rules! check_filter {
                ($filter:ty) => {
                    fingerprints.push(fingerprint(
                        format!("sst_{}_{compression:?}_{single_table}", stringify!($filter)),
                        &sstable_fixture::<$filter>(compression, single_table).await,
                    ));
                };
            }
            check_filter!(Xor8FilterBuilder);
            check_filter!(Xor16FilterBuilder);
            check_filter!(BlockedXor8FilterBuilder);
            check_filter!(BlockedXor16FilterBuilder);
        }
    }
    expect_test::expect![[r#"
        [
            (
                "sst_Xor8FilterBuilder_None_true",
                32437483,
                12794410097804486889,
            ),
            (
                "sst_Xor16FilterBuilder_None_true",
                32437736,
                8419537671942218398,
            ),
            (
                "sst_BlockedXor8FilterBuilder_None_true",
                32449933,
                17955418076010547000,
            ),
            (
                "sst_BlockedXor16FilterBuilder_None_true",
                32458358,
                18227206473273718116,
            ),
            (
                "sst_Xor8FilterBuilder_None_false",
                64480566,
                15102983699305105326,
            ),
            (
                "sst_Xor16FilterBuilder_None_false",
                64481041,
                5511926894421397356,
            ),
            (
                "sst_BlockedXor8FilterBuilder_None_false",
                64505502,
                14487003704105793094,
            ),
            (
                "sst_BlockedXor16FilterBuilder_None_false",
                64522351,
                11802838026014567674,
            ),
            (
                "sst_Xor8FilterBuilder_Lz4_true",
                14402455,
                6618143604050799991,
            ),
            (
                "sst_Xor16FilterBuilder_Lz4_true",
                14402708,
                4702333884567193352,
            ),
            (
                "sst_BlockedXor8FilterBuilder_Lz4_true",
                14414905,
                9333354081943674299,
            ),
            (
                "sst_BlockedXor16FilterBuilder_Lz4_true",
                14423330,
                504370265002530884,
            ),
            (
                "sst_Xor8FilterBuilder_Lz4_false",
                28410514,
                10565363676396744068,
            ),
            (
                "sst_Xor16FilterBuilder_Lz4_false",
                28410989,
                6681415297972566626,
            ),
            (
                "sst_BlockedXor8FilterBuilder_Lz4_false",
                28435450,
                1467906437796840738,
            ),
            (
                "sst_BlockedXor16FilterBuilder_Lz4_false",
                28452299,
                5832557848392325988,
            ),
            (
                "sst_Xor8FilterBuilder_Zstd_true",
                14324222,
                17683973029731682874,
            ),
            (
                "sst_Xor16FilterBuilder_Zstd_true",
                14324475,
                2147346609926646340,
            ),
            (
                "sst_BlockedXor8FilterBuilder_Zstd_true",
                14336672,
                14391203056768017149,
            ),
            (
                "sst_BlockedXor16FilterBuilder_Zstd_true",
                14345097,
                13417176695824456001,
            ),
            (
                "sst_Xor8FilterBuilder_Zstd_false",
                28254047,
                8702127424067086581,
            ),
            (
                "sst_Xor16FilterBuilder_Zstd_false",
                28254522,
                5312857137642899646,
            ),
            (
                "sst_BlockedXor8FilterBuilder_Zstd_false",
                28278983,
                14031853563171661171,
            ),
            (
                "sst_BlockedXor16FilterBuilder_Zstd_false",
                28295832,
                17084472181134684378,
            ),
        ]
    "#]]
    .assert_debug_eq(&fingerprints);
}

#[tokio::test]
async fn raw_block_encoding_compatibility() {
    let mut fingerprints = Vec::new();
    for compression in [
        CompressionAlgorithm::None,
        CompressionAlgorithm::Lz4,
        CompressionAlgorithm::Zstd,
    ] {
        let options = SstableBuilderOptions {
            block_capacity: 64 * 1024,
            compression_algorithm: compression,
            ..Default::default()
        };
        let new_builder = || {
            SstableBuilder::new(
                1,
                InMemWriter::from(&options),
                BlockedXor16FilterBuilder::create(options.filter_builder_options()),
                options.clone(),
                CompactionCatalogAgent::for_test(vec![0]),
                None,
            )
        };
        let keys: Vec<_> = (0u64..6)
            .map(|i| {
                let mut key = VirtualNode::ZERO.to_be_bytes().to_vec();
                key.extend_from_slice(&i.to_be_bytes());
                FullKey::for_test(TableId::default(), key, test_epoch(2))
            })
            .collect();
        let mut source = new_builder();
        for key in &keys[1..5] {
            source
                .add(key.to_ref(), HummockValue::put(&[42; 128][..]))
                .await
                .unwrap();
        }
        let (data, meta) = source.finish().await.unwrap().writer_output;
        assert_eq!(meta.block_metas.len(), 1);
        let block_meta = &meta.block_metas[0];
        let filter = XorFilterReader::new(&meta.bloom_filter, &meta.block_metas);
        // A small pending block forces decode/rebuild; a larger one allows raw transfer.
        for prefix_len in [1, 8192] {
            let mut builder = new_builder();
            let prefix = vec![0; prefix_len];
            builder
                .add(keys[0].to_ref(), HummockValue::put(&prefix[..]))
                .await
                .unwrap();
            let copied = builder
                .add_raw_block(
                    data.slice(
                        block_meta.offset as usize..(block_meta.offset + block_meta.len) as usize,
                    ),
                    filter.get_block_raw_filter(0),
                    FullKey::decode(&block_meta.smallest_key).to_vec(),
                    meta.largest_key.clone(),
                    block_meta.clone(),
                )
                .await
                .unwrap();
            assert_eq!(copied, prefix_len == 8192);
            // First append another version of the raw block's last user key, then a new key.
            let previous = FullKey::for_test(
                TableId::default(),
                keys[4].user_key.table_key.as_ref(),
                test_epoch(1),
            );
            builder.add(previous, HummockValue::Delete).await.unwrap();
            builder
                .add(keys[5].to_ref(), HummockValue::put(&[][..]))
                .await
                .unwrap();
            let (data, _) = builder.finish().await.unwrap().writer_output;
            fingerprints.push(fingerprint(
                format!("raw_{compression:?}_{prefix_len}"),
                &data,
            ));
        }
    }
    expect_test::expect![[r#"
        [
            (
                "raw_None_1",
                879,
                9936056481200114159,
            ),
            (
                "raw_None_8192",
                9407,
                4183736381145003073,
            ),
            (
                "raw_Lz4_1",
                346,
                6980407932593871415,
            ),
            (
                "raw_Lz4_8192",
                743,
                9725270222495448044,
            ),
            (
                "raw_Zstd_1",
                335,
                2049247274844760043,
            ),
            (
                "raw_Zstd_8192",
                670,
                822091869937286526,
            ),
        ]
    "#]]
    .assert_debug_eq(&fingerprints);
}
