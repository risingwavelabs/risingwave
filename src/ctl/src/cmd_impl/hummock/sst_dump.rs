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

use std::collections::HashMap;

use bytes::{Buf, Bytes};
use chrono::offset::Utc;
use chrono::DateTime;
use clap::Args;
use itertools::Itertools;
use risingwave_common::row::{Row, RowDeserializer};
use risingwave_common::types::to_text::ToText;
use risingwave_common::util::epoch::Epoch;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_frontend::TableCatalog;
use risingwave_hummock_sdk::compaction_group::hummock_version_ext::HummockVersionExt;
use risingwave_hummock_sdk::key::FullKey;
use risingwave_object_store::object::BlockLocation;
use risingwave_pb::hummock::{Level, SstableInfo};
use risingwave_rpc_client::MetaClient;
use risingwave_storage::hummock::value::HummockValue;
use risingwave_storage::hummock::{
    Block, BlockHolder, BlockIterator, CompressionAlgorithm, Sstable, SstableStore,
};
use risingwave_storage::monitor::StoreLocalStatistic;

use crate::CtlContext;

type TableData = HashMap<u32, TableCatalog>;

#[derive(Args, Debug)]
pub struct SstDumpArgs {
    #[clap(short, long = "sst-id")]
    sst_id: Option<u64>,
    #[clap(short, long = "block-id")]
    block_id: Option<u64>,
    #[clap(short = 'p', long = "print-entries")]
    print_entries: bool,
    #[clap(short = 'l', long = "print-level-info")]
    print_level: bool,
}

fn print_level(level: &Level) {
    println!("Level Type: {}", level.level_type);
    println!("Level Idx: {}", level.level_idx);
    if level.level_idx == 0 {
        println!("L0 Sub-Level Idx: {}", level.sub_level_id);
    }
}

pub async fn sst_dump(context: &CtlContext, args: SstDumpArgs) -> anyhow::Result<()> {
    println!("Start sst dump with args: {:?}", args);
    // Retrieves the Sstable store so we can access the SstableMeta
    let meta_client = context.meta_client().await?;
    let hummock = context.hummock_store().await?;
    let version = hummock.inner().get_pinned_version().version();

    let table_data = load_table_schemas(&meta_client).await?;
    let sstable_store = &*hummock.sstable_store();

    // TODO: We can avoid reading meta if `print_level` is false with the new block format.
    for level in version.get_combined_levels() {
        for sstable_info in &level.table_infos {
            if let Some(sst_id) = &args.sst_id {
                if *sst_id == sstable_info.id {
                    if args.print_level {
                        print_level(level);
                    }

                    sst_dump_via_sstable_store(
                        sstable_store,
                        sstable_info.id,
                        sstable_info.meta_offset,
                        sstable_info.file_size,
                        &table_data,
                        &args,
                    )
                    .await?;
                    return Ok(());
                }
            } else {
                if args.print_level {
                    print_level(level);
                }

                sst_dump_via_sstable_store(
                    sstable_store,
                    sstable_info.id,
                    sstable_info.meta_offset,
                    sstable_info.file_size,
                    &table_data,
                    &args,
                )
                .await?;
            }
        }
    }
    Ok(())
}

pub async fn sst_dump_via_sstable_store(
    sstable_store: &SstableStore,
    sst_id: u64,
    meta_offset: u64,
    file_size: u64,
    table_data: &TableData,
    args: &SstDumpArgs,
) -> anyhow::Result<()> {
    let sstable_info = SstableInfo {
        id: sst_id,
        meta_offset,
        file_size,
        ..Default::default()
    };
    let sstable_cache = sstable_store
        .sstable(&sstable_info, &mut StoreLocalStatistic::default())
        .await?;
    let sstable = sstable_cache.value().as_ref();
    let sstable_meta = &sstable.meta;

    println!("SST id: {}", sst_id);
    println!("-------------------------------------");
    println!("File Size: {}", sstable.estimate_size());

    println!("Key Range:");
    println!(
        "\tleft:\t{:?}\n\tright:\t{:?}\n\t",
        sstable_meta.smallest_key, sstable_meta.largest_key,
    );

    println!("Estimated Table Size: {}", sstable_meta.estimated_size);
    println!("Bloom Filter Size: {}", sstable_meta.bloom_filter.len());
    println!("Key Count: {}", sstable_meta.key_count);
    println!("Version: {}", sstable_meta.version);

    println!("SST Block Count: {}", sstable.block_count());
    for i in 0..sstable.block_count() {
        if let Some(block_id) = &args.block_id {
            if *block_id == i as u64 {
                print_block(i, table_data, sstable_store, sstable, args).await?;
                return Ok(());
            }
        } else {
            print_block(i, table_data, sstable_store, sstable, args).await?;
        }
    }
    Ok(())
}

/// Determine all database tables and adds their information into a hash table with the table-ID as
/// key.
async fn load_table_schemas(meta_client: &MetaClient) -> anyhow::Result<TableData> {
    let mut tables = HashMap::new();

    let mvs = meta_client.risectl_list_state_tables().await?;
    mvs.iter().for_each(|tbl| {
        tables.insert(tbl.id, tbl.into());
    });

    Ok(tables)
}

/// Prints a block of a given SST including all contained KV-pairs.
async fn print_block(
    block_idx: usize,
    table_data: &TableData,
    sstable_store: &SstableStore,
    sst: &Sstable,
    args: &SstDumpArgs,
) -> anyhow::Result<()> {
    println!("\tBlock {}", block_idx);
    println!("\t-----------");

    let block_meta = &sst.meta.block_metas[block_idx];
    let data_path = sstable_store.get_sst_data_path(sst.id);

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

    if args.print_entries {
        print_kv_pairs(
            block_data,
            table_data,
            block_meta.uncompressed_size as usize,
        )?;
    }

    Ok(())
}

/// Prints the data of KV-Pairs of a given block out to the terminal.
fn print_kv_pairs(
    block_data: Bytes,
    table_data: &TableData,
    uncompressed_capacity: usize,
) -> anyhow::Result<()> {
    println!("\tKV-Pairs:");

    let block = Box::new(Block::decode(block_data, uncompressed_capacity).unwrap());
    let holder = BlockHolder::from_owned_block(block);
    let mut block_iter = BlockIterator::new(holder);
    block_iter.seek_to_first();

    while block_iter.is_valid() {
        let raw_full_key = block_iter.key();
        let full_key = block_iter.key();
        let raw_user_key = full_key.user_key.encode();

        let full_val = block_iter.value();
        let humm_val = HummockValue::from_slice(block_iter.value())?;
        let (is_put, user_val) = match humm_val {
            HummockValue::Put(uval) => (true, uval),
            HummockValue::Delete => (false, &[] as &[u8]),
        };

        let epoch = Epoch::from(full_key.epoch);
        let date_time = DateTime::<Utc>::from(epoch.as_system_time());

        println!(
            "\t\t  full key: {:02x?}, len={}",
            raw_full_key,
            raw_full_key.len()
        );
        println!("\t\tfull value: {:02x?}, len={}", full_val, full_val.len());
        println!("\t\t  user key: {:02x?}", raw_user_key);
        println!("\t\tuser value: {:02x?}", user_val);
        println!("\t\t     epoch: {} ({})", epoch, date_time);
        println!("\t\t      type: {}", if is_put { "Put" } else { "Delete" });

        print_table_column(full_key, user_val, table_data, is_put)?;

        println!();

        block_iter.next();
    }

    Ok(())
}

/// If possible, prints information about the table, column, and stored value.
fn print_table_column(
    full_key: FullKey<&[u8]>,
    user_val: &[u8],
    table_data: &TableData,
    is_put: bool,
) -> anyhow::Result<()> {
    let table_id = full_key.user_key.table_id.table_id();

    print!("\t\t     table: {} - ", table_id);
    let table_catalog = match table_data.get(&table_id) {
        None => {
            // Table may have been dropped.
            println!("(unknown)");
            return Ok(());
        }
        Some(table) => table,
    };
    println!("{}", table_catalog.name);
    if !is_put {
        return Ok(());
    }

    let column_desc = table_catalog
        .value_indices
        .iter()
        .map(|idx| table_catalog.columns[*idx].column_desc.name.clone())
        .collect_vec();
    let data_types = table_catalog
        .value_indices
        .iter()
        .map(|idx| table_catalog.columns[*idx].data_type().clone())
        .collect_vec();
    let row_deserializer = RowDeserializer::new(data_types);
    let row = row_deserializer.deserialize(user_val)?;
    for (c, v) in column_desc.iter().zip_eq_fast(row.iter()) {
        println!("\t\t    column: {} {}", c, v.to_text());
    }

    Ok(())
}
