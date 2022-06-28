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

use bytes::Buf;
use risingwave_hummock_sdk::compaction_group::hummock_version_ext::HummockVersionExt;
use risingwave_object_store::object::BlockLocation;
use risingwave_rpc_client::HummockMetaClient;
use risingwave_storage::hummock::CompressionAlgorithm;
use risingwave_storage::monitor::StoreLocalStatistic;

use crate::common::HummockServiceOpts;

pub async fn sst_dump() -> anyhow::Result<()> {
    // Retrieves the SSTable store so we can access the SSTableMeta
    let mut hummock_opts = HummockServiceOpts::from_env()?;
    let (meta_client, hummock) = hummock_opts.create_hummock_store().await?;
    let sstable_store = &*hummock.sstable_store();

    // Retrieves the latest HummockVersion from the meta client so we can access the SSTableInfo
    let version = meta_client.pin_version(u64::MAX).await?;

    let sstable_id_infos = meta_client.list_sstable_id_infos(version.id).await?;
    let mut sstable_id_infos_iter = sstable_id_infos.iter();

    for level in version.get_combined_levels() {
        for sstable_info in level.table_infos.clone() {
            let id = sstable_info.id;

            let sstable_cache = sstable_store
                .sstable(id, &mut StoreLocalStatistic::default())
                .await?;
            let sstable = sstable_cache.value().as_ref();
            let sstable_meta = &sstable.meta;

            let sstable_id_info = sstable_id_infos_iter.next().unwrap();

            println!("SST id: {}", id);
            println!("-------------------------------------");
            println!(
                "Creation Timestamp: {}",
                sstable_id_info.id_create_timestamp
            );
            println!(
                "Creation Timestamp (Meta): {}",
                sstable_id_info.meta_create_timestamp
            );
            println!(
                "Deletion Timestamp (Meta): {}",
                sstable_id_info.meta_delete_timestamp
            );
            println!("File Size: {}", sstable_info.file_size);

            if let Some(key_range) = sstable_info.key_range {
                println!("Key Range:");
                println!(
                    "\tleft:\t{:?}\n\tright:\t{:?}\n\tinf:\t{:?}",
                    key_range.left, key_range.right, key_range.inf
                );
            } else {
                println!("Key Range: None");
            }

            let data_path = sstable_store.get_sst_data_path(id);
            println!("Block Metadata:");
            for (i, block_meta) in sstable_meta.block_metas.iter().enumerate() {
                // Retrieve encoded block data in bytes
                let store = sstable_store.store();
                let block_loc = BlockLocation {
                    offset: block_meta.offset as usize,
                    size: block_meta.len as usize,
                };
                let block_data = store.read(&data_path, Some(block_loc)).await?;

                // Retrieve checksum and compression algorithm used from the encoded block data
                let len = block_data.len();
                let checksum = (&block_data[len - 8..]).get_u64_le();
                let compression = CompressionAlgorithm::decode(&mut &block_data[len - 9..len - 8])?;

                println!(
                    "\tBlock {}, Offset: {}, Size: {}, Checksum: {}, Compression Algorithm: {:?}",
                    i, block_meta.offset, block_meta.len, checksum, compression
                );
            }

            println!("Estimated Table Size: {}", sstable_meta.estimated_size);
            println!("Bloom Filter Size: {}", sstable_meta.bloom_filter.len());
            println!("Key Count: {}", sstable_meta.key_count);
            println!("Version: {}", sstable_meta.version);
            println!();
        }
    }

    meta_client.unpin_version(&[version.id]).await?;

    hummock_opts.shutdown().await;
    Ok(())
}
