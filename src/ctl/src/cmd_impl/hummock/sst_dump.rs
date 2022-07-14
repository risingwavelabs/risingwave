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

use bytes::Buf;
use risingwave_common::util::value_encoding::deserialize_cell;
use risingwave_frontend::catalog::TableCatalog;
use risingwave_hummock_sdk::compaction_group::hummock_version_ext::HummockVersionExt;
use risingwave_hummock_sdk::key::{get_epoch, get_table_id, user_key};
use risingwave_object_store::object::{BlockLocation, ObjectStore};
use risingwave_rpc_client::HummockMetaClient;
use risingwave_storage::hummock::value::HummockValue;
use risingwave_storage::hummock::{Block, BlockHolder, BlockIterator, CompressionAlgorithm};
use risingwave_storage::monitor::StoreLocalStatistic;

use crate::common::{HummockServiceOpts, MetaServiceOpts};

pub async fn sst_dump() -> anyhow::Result<()> {
    // Retrieves the SSTable store so we can access the SSTableMeta
    let mut hummock_opts = HummockServiceOpts::from_env()?;
    let (meta_client, hummock) = hummock_opts.create_hummock_store().await?;
    let sstable_store = &*hummock.sstable_store();

    // Retrieves the latest HummockVersion from the meta client so we can access the SSTableInfo
    let version = meta_client.pin_version(u64::MAX).await?;

    // Collect all SstableIdInfos. We need them for time stamps.
    let mut id_info_map = HashMap::new();
    let sstable_id_infos = meta_client.list_sstable_id_infos(version.id).await?;
    sstable_id_infos.iter().for_each(|id_info| {
        id_info_map.insert(id_info.id, id_info);
    });

    // Determine all database tables ...
    let meta_opts = MetaServiceOpts::from_env()?;
    let meta = meta_opts.create_meta_client().await?;
    let mvs = meta.risectl_list_state_tables().await?;

    // ... and add a list of their types into a hash table.
    let mut column_table = HashMap::new();
    mvs.iter().for_each(|tbl| {
        let mut col_list = vec![];
        TableCatalog::from(tbl).columns.iter().for_each(|clm| {
            col_list.push((
                clm.data_type().clone(),
                String::from(clm.name()),
                clm.is_hidden().clone(),
            ));
        });
        column_table.insert(tbl.id, (tbl.name.clone(), col_list));
    });

    for level in version.get_combined_levels() {
        for sstable_info in &level.table_infos {
            let id = sstable_info.id;

            let sstable_cache = sstable_store
                .sstable(id, &mut StoreLocalStatistic::default())
                .await?;
            let sstable = sstable_cache.value().as_ref();
            let sstable_meta = &sstable.meta;

            let sstable_id_info = id_info_map[&id];

            println!("SST id: {}", id);
            println!("-------------------------------------");
            println!("Level: {}", level.level_type);
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

            if let Some(key_range) = sstable_info.key_range.as_ref() {
                println!("Key Range:");
                println!(
                    "\tleft:\t{:?}\n\tright:\t{:?}\n\tinf:\t{:?}",
                    key_range.left, key_range.right, key_range.inf
                );
            } else {
                println!("Key Range: None");
            }

            let data_path = sstable_store.get_sst_data_path(id);
            println!("Blocks:");
            for (i, block_meta) in sstable_meta.block_metas.iter().enumerate() {
                println!("\tBlock {}", i);
                println!("\t-----------");

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
                    "\tOffset: {}, Size: {}, Checksum: {}, Compression Algorithm: {:?}",
                    block_meta.offset, block_meta.len, checksum, compression
                );

                println!("\tKV-Pairs:");

                let block = Box::new(Block::decode(block_data).unwrap());
                let holder = BlockHolder::from_owned_block(block);
                let mut block_iter = BlockIterator::new(holder);
                block_iter.seek_to_first();

                let mut column_idx: usize = 0;
                while block_iter.is_valid() {
                    let full_key = block_iter.key();
                    let user_key = user_key(full_key);

                    let full_val = block_iter.value();
                    let humm_val = HummockValue::from_slice(block_iter.value())?;
                    let (isPut, val_meta, user_val) = match humm_val {
                        HummockValue::Put(meta, uval) => (true, meta.vnode, uval),
                        HummockValue::Delete(meta) => (false, meta.vnode, &[] as &[u8]),
                    };

                    let epoch = get_epoch(full_key);

                    println!("\t\t  full key: {:02x?}", full_key);
                    println!("\t\tfull value: {:02x?}", full_val);
                    println!("\t\t  user key: {:02x?}", user_key);
                    println!("\t\tuser value: {:02x?}", user_val);
                    println!("\t\t     epoch: {}", epoch);
                    println!("\t\t      type: {}", if isPut { "Put" } else { "Delete" });
                    println!("\t\tvalue-meta: {}", val_meta);

                    if let Some(table_id) = get_table_id(full_key) {
                        let (table_name, columns) = &column_table.get(&table_id).unwrap();
                        println!("\t\t     table: {} - {}", table_id, table_name);

                        // Check if new row.
                        if isPut {
                            if user_val.len() == 0
                                && (&user_key[user_key.len() - 4..]).get_i32() == 0x7fffffff
                            {
                                // New row. Reset column index.
                                column_idx = 0;
                            } else {
                                let (data_type, name, is_hidden) = &columns[column_idx];
                                let datum = deserialize_cell(user_val, data_type).unwrap().unwrap();
                                println!(
                                    "\t\t    column: {} {}",
                                    name,
                                    if is_hidden.clone() { "(hidden)" } else { "" }
                                );
                                println!("\t\t     datum: {:?}", datum);
                                column_idx += 1;
                            }
                        }
                    }

                    println!();

                    block_iter.next();
                }
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
