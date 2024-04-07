// Copyright 2024 RisingWave Labs
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

use std::sync::Arc;
use std::time::Instant;

use anyhow::{anyhow, Result};
use futures::{pin_mut, StreamExt};
use risingwave_common::buffer::BitmapBuilder;
use risingwave_common::catalog::ColumnId;
use risingwave_common::hash::VirtualNode;
use risingwave_frontend::TableCatalog;
use risingwave_hummock_sdk::HummockReadEpoch;
use risingwave_rpc_client::MetaClient;
use risingwave_storage::hummock::HummockStorage;
use risingwave_storage::monitor::MonitoredStateStore;
use risingwave_storage::store::PrefetchOptions;
use risingwave_storage::table::batch_table::storage_table::StorageTable;
use risingwave_storage::table::TableDistribution;
use risingwave_storage::StateStore;
use risingwave_stream::common::table::state_table::StateTable;

use crate::common::HummockServiceOpts;
use crate::CtlContext;

pub async fn get_table_catalog(meta: MetaClient, mv_name: String) -> Result<TableCatalog> {
    let mvs = meta.risectl_list_state_tables().await?;
    let mv = mvs
        .iter()
        .find(|x| x.name == mv_name)
        .ok_or_else(|| anyhow!("mv not found"))?
        .clone();
    Ok(TableCatalog::from(&mv))
}

pub async fn get_table_catalog_by_id(meta: MetaClient, table_id: u32) -> Result<TableCatalog> {
    let mvs = meta.risectl_list_state_tables().await?;
    let mv = mvs
        .iter()
        .find(|x| x.id == table_id)
        .ok_or_else(|| anyhow!("mv not found"))?
        .clone();
    Ok(TableCatalog::from(&mv))
}

pub fn print_table_catalog(table: &TableCatalog) {
    println!("{:#?}", table);
}

pub async fn make_state_table<S: StateStore>(hummock: S, table: &TableCatalog) -> StateTable<S> {
    StateTable::new_with_distribution(
        hummock,
        table.id,
        table
            .columns()
            .iter()
            .map(|x| x.column_desc.clone())
            .collect(),
        table.pk().iter().map(|x| x.order_type).collect(),
        table.pk().iter().map(|x| x.column_index).collect(),
        TableDistribution::all(table.distribution_key().to_vec()), // scan all vnodes
        Some(table.value_indices.clone()),
    )
    .await
}

pub fn make_storage_table<S: StateStore>(
    hummock: S,
    table: &TableCatalog,
    output_columns_ids: Option<Vec<i32>>,
    vnodes: Option<Vec<i32>>,
) -> Result<StorageTable<S>> {
    let output_columns_ids = match output_columns_ids {
        None => table
            .columns()
            .iter()
            .map(|x| x.column_desc.column_id)
            .collect(),
        Some(output_columns_ids) => output_columns_ids.into_iter().map(ColumnId::new).collect(),
    };
    let vnodes = match vnodes {
        None => TableDistribution::all_vnodes(),
        Some(vnodes) => {
            let mut bitmap = BitmapBuilder::zeroed(VirtualNode::COUNT);
            for v in vnodes {
                bitmap.set(v as usize, true);
            }
            Arc::new(bitmap.finish())
        }
    };
    Ok(StorageTable::new_partial(
        hummock,
        output_columns_ids,
        Some(vnodes),
        &table.table_desc().try_to_protobuf()?,
    ))
}

pub async fn scan(
    context: &CtlContext,
    mv_name: String,
    data_dir: Option<String>,
    output_columns_ids: Option<Vec<i32>>,
    vnodes: Option<Vec<i32>>,
    silent: bool,
) -> Result<()> {
    let meta_client = context.meta_client().await?;
    let hummock = context
        .hummock_store(HummockServiceOpts::from_env(data_dir)?)
        .await?;
    let table = get_table_catalog(meta_client, mv_name).await?;
    do_scan(table, hummock, output_columns_ids, vnodes, silent).await
}

pub async fn scan_id(
    context: &CtlContext,
    table_id: u32,
    data_dir: Option<String>,
    output_columns_ids: Option<Vec<i32>>,
    vnodes: Option<Vec<i32>>,
    silent: bool,
) -> Result<()> {
    let meta_client = context.meta_client().await?;
    let hummock = context
        .hummock_store(HummockServiceOpts::from_env(data_dir)?)
        .await?;
    let table = get_table_catalog_by_id(meta_client, table_id).await?;
    do_scan(table, hummock, output_columns_ids, vnodes, silent).await
}

async fn do_scan(
    table: TableCatalog,
    hummock: MonitoredStateStore<HummockStorage>,
    output_columns_ids: Option<Vec<i32>>,
    vnodes: Option<Vec<i32>>,
    silent: bool,
) -> Result<()> {
    print_table_catalog(&table);

    if let Some(ref output_columns_ids) = output_columns_ids {
        println!(
            "output column ids: {}",
            output_columns_ids
                .iter()
                .map(|num| num.to_string())
                .collect::<Vec<String>>()
                .join(",")
        );
    }

    if !silent {
        println!("Rows:");
    }
    let read_epoch = hummock.inner().get_pinned_version().max_committed_epoch();
    let storage_table = make_storage_table(hummock, &table, output_columns_ids, vnodes)?;
    let instant = Instant::now();
    let stream = storage_table
        .batch_iter(
            HummockReadEpoch::Committed(read_epoch),
            true,
            PrefetchOptions::prefetch_for_large_range_scan(),
        )
        .await?;
    let create_duration = instant.elapsed();
    let mut counter = 0;
    pin_mut!(stream);
    while let Some(item) = stream.next().await {
        if !silent {
            println!("{:?}", item?.into_owned_row());
        }
        counter += 1;
    }
    let scan_duration = instant.elapsed();
    println!(
        "{counter} rows in total. create={}ms, scan={}ms",
        create_duration.as_millis(),
        scan_duration.as_millis()
    );
    Ok(())
}
