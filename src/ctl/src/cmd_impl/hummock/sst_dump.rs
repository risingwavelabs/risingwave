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

use risingwave_rpc_client::HummockMetaClient;
use risingwave_storage::monitor::StoreLocalStatistic;

use crate::common::{HummockServiceOpts, MetaServiceOpts};

pub async fn sst_dump() -> anyhow::Result<()> {
    // Retrieves the latest HummockVersion from the meta client so we can access the SSTableInfo
    let meta_opts = MetaServiceOpts::from_env()?;
    let meta_client = meta_opts.create_meta_client().await?;
    let version = meta_client.pin_version(u64::MAX).await?;

    // Retrieves the SSTable store so we can access the SSTableMeta
    let hummock_opts = HummockServiceOpts::from_env()?;
    let hummock = hummock_opts.create_hummock_store().await?;
    let sstable_store = &*hummock.inner().sstable_store();

    for level in version.levels {
        for sstable_info in level.table_infos {
            let id = sstable_info.id;

            let sstable_cache = sstable_store
                .sstable(id, &mut StoreLocalStatistic::default())
                .await?;
            let sstable_meta = &sstable_cache.value().meta;

            println!("SST id: {}", id);
            println!("-------------------------------------");
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

            println!("VNodes:");
            for vnode_bitmap in sstable_info.vnode_bitmaps {
                let mut vnodes = vec![];
                for (i, bitmap_part) in vnode_bitmap.bitmap.iter().enumerate() {
                    for j in 0..8 {
                        if (bitmap_part >> j) & 1 == 1 {
                            vnodes.push(8 * i + j);
                        }
                    }
                }

                println!(
                    "\tTable id: {}, VNodes: {:?}",
                    vnode_bitmap.table_id, vnodes
                );
            }

            println!("Block Metadata:");
            for (i, block_meta) in sstable_meta.block_metas.iter().enumerate() {
                println!(
                    "\tBlock {}, Size: {}, Offset: {}",
                    i, block_meta.len, block_meta.offset
                );
            }

            println!("Estimated Table Size: {}", sstable_meta.estimated_size);
            println!("Bloom Filter Size: {}", sstable_meta.bloom_filter.len());
            println!("Key Count: {}", sstable_meta.key_count);
            println!("Version: {}", sstable_meta.version);
        }
    }

    meta_client.unpin_version(&[version.id]).await?;

    Ok(())
}
