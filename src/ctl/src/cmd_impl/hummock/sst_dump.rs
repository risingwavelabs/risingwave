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
use std::sync::Arc;

use bytes::{Buf, Bytes};
use chrono::offset::Utc;
use chrono::DateTime;
use clap::Args;
use itertools::Itertools;
use risingwave_common::types::ToText;
use risingwave_common::util::epoch::Epoch;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_common::util::value_encoding::column_aware_row_encoding::ColumnAwareSerde;
use risingwave_common::util::value_encoding::{BasicSerde, EitherSerde, ValueRowDeserializer};
use risingwave_frontend::TableCatalog;
use risingwave_hummock_sdk::compaction_group::hummock_version_ext::HummockVersionExt;
use risingwave_hummock_sdk::key::FullKey;
use risingwave_hummock_sdk::HummockSstableObjectId;
use risingwave_object_store::object::{BlockLocation, ObjectMetadata, ObjectStoreImpl};
use risingwave_pb::hummock::{Level, SstableInfo};
use risingwave_rpc_client::MetaClient;
use risingwave_storage::hummock::value::HummockValue;
use risingwave_storage::hummock::{
    Block, BlockHolder, BlockIterator, CompressionAlgorithm, Sstable, SstableStore,
};
use risingwave_storage::monitor::StoreLocalStatistic;
use risingwave_storage::value_serde::ValueRowSerdeNew;

use crate::common::HummockServiceOpts;
use crate::CtlContext;

type TableData = HashMap<u32, TableCatalog>;

#[derive(Args, Debug)]
pub struct SstDumpArgs {
    #[clap(short, long = "object-id")]
    object_id: Option<u64>,
    #[clap(short, long = "block-id")]
    block_id: Option<u64>,
    #[clap(short = 'p', long = "print-entry")]
    print_entry: bool,
    #[clap(short = 'l', long = "print-level")]
    print_level: bool,
    #[clap(short = 't', long = "print-table")]
    print_table: bool,
    #[clap(short = 'd')]
    data_dir: Option<String>,
}

pub async fn sst_dump(context: &CtlContext, args: SstDumpArgs) -> anyhow::Result<()> {
    println!("Start sst dump with args: {:?}", args);
    let table_data = if args.print_entry && args.print_table {
        let meta_client = context.meta_client().await?;
        load_table_schemas(&meta_client).await?
    } else {
        TableData::default()
    };
    if args.print_level {
        // Level information is retrieved from meta service
        let hummock = context
            .hummock_store(HummockServiceOpts::from_env(args.data_dir.clone())?)
            .await?;
        let version = hummock.inner().get_pinned_version().version();
        let sstable_store = hummock.sstable_store();
        for level in version.get_combined_levels() {
            for sstable_info in &level.table_infos {
                if let Some(object_id) = &args.object_id {
                    if *object_id == sstable_info.get_object_id() {
                        print_level(level, sstable_info);
                        sst_dump_via_sstable_store(
                            &sstable_store,
                            sstable_info.get_object_id(),
                            sstable_info.meta_offset,
                            sstable_info.file_size,
                            &table_data,
                            &args,
                        )
                        .await?;
                        return Ok(());
                    }
                } else {
                    print_level(level, sstable_info);
                    sst_dump_via_sstable_store(
                        &sstable_store,
                        sstable_info.get_object_id(),
                        sstable_info.meta_offset,
                        sstable_info.file_size,
                        &table_data,
                        &args,
                    )
                    .await?;
                }
            }
        }
    } else {
        // Object information is retrieved from object store. Meta service is not required.
        let hummock_service_opts = HummockServiceOpts::from_env(args.data_dir.clone())?;
        let sstable_store = hummock_service_opts.create_sstable_store().await?;
        if let Some(obj_id) = &args.object_id {
            let obj_store = sstable_store.store();
            let obj_path = sstable_store.get_sst_data_path(*obj_id);
            let obj = &obj_store.list(&obj_path).await?[0];
            print_object(obj);
            let meta_offset = get_meta_offset_from_object(obj, obj_store.as_ref()).await?;
            let obj_id = sstable_store.get_object_id_from_path(&obj.key);
            sst_dump_via_sstable_store(
                &sstable_store,
                obj_id,
                meta_offset,
                obj.total_size as u64,
                &table_data,
                &args,
            )
            .await?;
        } else {
            let objects = sstable_store.list_ssts_from_object_store().await?;
            for obj in objects {
                print_object(&obj);
                let meta_offset =
                    get_meta_offset_from_object(&obj, sstable_store.store().as_ref()).await?;
                let obj_id = sstable_store.get_object_id_from_path(&obj.key);
                sst_dump_via_sstable_store(
                    &sstable_store,
                    obj_id,
                    meta_offset,
                    obj.total_size as u64,
                    &table_data,
                    &args,
                )
                .await?;
            }
        }
    }

    Ok(())
}

fn print_level(level: &Level, sst_info: &SstableInfo) {
    println!("Level Type: {}", level.level_type);
    println!("Level Idx: {}", level.level_idx);
    if level.level_idx == 0 {
        println!("L0 Sub-Level Idx: {}", level.sub_level_id);
    }
    println!("SST id: {}", sst_info.sst_id);
    println!("SST table_ids: {:?}", sst_info.table_ids);
}

fn print_object(obj: &ObjectMetadata) {
    println!("Object Key: {}", obj.key);
    println!("Object Size: {}", obj.total_size);
    println!("Object Last Modified: {}", obj.last_modified);
}

async fn get_meta_offset_from_object(
    obj: &ObjectMetadata,
    obj_store: &ObjectStoreImpl,
) -> anyhow::Result<u64> {
    let meta_offset_loc = BlockLocation {
        offset: obj.total_size
            - (
                // version, magic
                2 * std::mem::size_of::<u32>() +
                // footer, checksum
                2 * std::mem::size_of::<u64>()
            ),
        size: std::mem::size_of::<u64>(),
    };
    Ok(obj_store
        .read(&obj.key, Some(meta_offset_loc))
        .await?
        .get_u64_le())
}

pub async fn sst_dump_via_sstable_store(
    sstable_store: &SstableStore,
    object_id: HummockSstableObjectId,
    meta_offset: u64,
    file_size: u64,
    table_data: &TableData,
    args: &SstDumpArgs,
) -> anyhow::Result<()> {
    let sstable_info = SstableInfo {
        object_id,
        file_size,
        meta_offset,
        ..Default::default()
    };
    let sstable_cache = sstable_store
        .sstable(&sstable_info, &mut StoreLocalStatistic::default())
        .await?;
    let sstable = sstable_cache.value().as_ref();
    let sstable_meta = &sstable.meta;
    let smallest_key = FullKey::decode(&sstable_meta.smallest_key);
    let largest_key = FullKey::decode(&sstable_meta.largest_key);

    println!("SST object id: {}", object_id);
    println!("-------------------------------------");
    println!("File Size: {}", sstable.estimate_size());

    println!("Key Range:");
    println!(
        "\tleft:\t{:?}\n\tright:\t{:?}\n\t",
        smallest_key, largest_key,
    );

    println!("Estimated Table Size: {}", sstable_meta.estimated_size);
    println!("Bloom Filter Size: {}", sstable_meta.bloom_filter.len());
    println!("Key Count: {}", sstable_meta.key_count);
    println!("Version: {}", sstable_meta.version);
    println!(
        "Monotonoic Deletes Count: {}",
        sstable_meta.monotonic_tombstone_events.len()
    );
    for monotonic_delete in &sstable_meta.monotonic_tombstone_events {
        println!("\tevent key: {:?}", monotonic_delete.event_key);
        println!("\tnew epoch: {:?}", monotonic_delete.new_epoch);
    }

    println!("Block Count: {}", sstable.block_count());
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
    let smallest_key = FullKey::decode(&block_meta.smallest_key);
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
        "\tOffset: {}, Size: {}, Checksum: {}, Compression Algorithm: {:?}, Smallest Key: {:?}",
        block_meta.offset, block_meta.len, checksum, compression, smallest_key
    );

    if args.print_entry {
        print_kv_pairs(
            block_data,
            table_data,
            block_meta.uncompressed_size as usize,
            args,
        )?;
    }

    Ok(())
}

/// Prints the data of KV-Pairs of a given block out to the terminal.
fn print_kv_pairs(
    block_data: Bytes,
    table_data: &TableData,
    uncompressed_capacity: usize,
    args: &SstDumpArgs,
) -> anyhow::Result<()> {
    println!("\tKV-Pairs:");

    let block = Box::new(Block::decode(block_data, uncompressed_capacity).unwrap());
    let holder = BlockHolder::from_owned_block(block);
    let mut block_iter = BlockIterator::new(holder);
    block_iter.seek_to_first();

    while block_iter.is_valid() {
        let full_key = block_iter.key();
        let full_val = block_iter.value();
        let humm_val = HummockValue::from_slice(full_val)?;

        let epoch = Epoch::from(full_key.epoch);
        let date_time = DateTime::<Utc>::from(epoch.as_system_time());

        println!("\t\t   key: {:?}, len={}", full_key, full_key.encoded_len());
        println!("\t\t value: {:?}, len={}", humm_val, humm_val.encoded_len());
        println!("\t\t epoch: {} ({})", epoch, date_time);

        if args.print_table {
            print_table_column(full_key, humm_val, table_data)?;
        }

        println!();

        block_iter.next();
    }

    Ok(())
}

/// If possible, prints information about the table, column, and stored value.
fn print_table_column(
    full_key: FullKey<&[u8]>,
    humm_val: HummockValue<&[u8]>,
    table_data: &TableData,
) -> anyhow::Result<()> {
    let table_id = full_key.user_key.table_id.table_id();

    print!("\t\t table: id={}, ", table_id);
    let table_catalog = match table_data.get(&table_id) {
        None => {
            // Table may have been dropped.
            println!("(unknown)");
            return Ok(());
        }
        Some(table) => table,
    };
    println!(
        "name={}, version={:?}",
        table_catalog.name,
        table_catalog.version()
    );

    if let Some(user_val) = humm_val.into_user_value() {
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
        let row_deserializer: EitherSerde = if table_catalog.version().is_some() {
            ColumnAwareSerde::new(
                Arc::from(value_indices.into_boxed_slice()),
                Arc::from(data_types.into_boxed_slice()),
                Arc::from(table_columns.into_boxed_slice()),
            )
            .into()
        } else {
            BasicSerde::new(
                Arc::from(value_indices.into_boxed_slice()),
                Arc::from(data_types.into_boxed_slice()),
                Arc::from(table_columns.into_boxed_slice()),
            )
            .into()
        };
        let row = row_deserializer.deserialize(user_val)?;
        for (c, v) in column_desc.iter().zip_eq_fast(row.iter()) {
            println!(
                "\t\tcolumn: {} {:?}",
                c,
                v.as_ref().map(|v| v.as_scalar_ref_impl().to_text())
            );
        }
    }

    Ok(())
}
