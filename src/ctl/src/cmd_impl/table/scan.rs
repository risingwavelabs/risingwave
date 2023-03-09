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

use anyhow::{anyhow, Result};
use futures::{pin_mut, StreamExt};
use risingwave_common::catalog::TableOption;
use risingwave_frontend::TableCatalog;
use risingwave_hummock_sdk::HummockReadEpoch;
use risingwave_rpc_client::MetaClient;
use risingwave_storage::hummock::HummockStorage;
use risingwave_storage::monitor::MonitoredStateStore;
use risingwave_storage::store::PrefetchOptions;
use risingwave_storage::table::batch_table::storage_table::StorageTable;
use risingwave_storage::table::Distribution;
use risingwave_storage::StateStore;
use risingwave_stream::common::table::state_table::StateTable;

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
        Distribution::all_vnodes(table.distribution_key().to_vec()), // scan all vnodes
        Some(table.value_indices.clone()),
    )
    .await
}

pub fn make_storage_table<S: StateStore>(hummock: S, table: &TableCatalog) -> StorageTable<S> {
    StorageTable::new_partial(
        hummock,
        table.id,
        table
            .columns()
            .iter()
            .map(|x| x.column_desc.clone())
            .collect(),
        table
            .columns()
            .iter()
            .map(|x| x.column_desc.column_id)
            .collect(),
        table.pk().iter().map(|x| x.order_type).collect(),
        table.pk().iter().map(|x| x.column_index).collect(),
        Distribution::all_vnodes(table.distribution_key().to_vec()),
        TableOption::build_table_option(&HashMap::new()),
        table.value_indices.clone(),
        table.read_prefix_len_hint,
    )
}

pub async fn scan(context: &CtlContext, mv_name: String) -> Result<()> {
    let meta_client = context.meta_client().await?;
    let hummock = context.hummock_store().await?;
    let table = get_table_catalog(meta_client, mv_name).await?;
    do_scan(table, hummock).await
}

pub async fn scan_id(context: &CtlContext, table_id: u32) -> Result<()> {
    let meta_client = context.meta_client().await?;
    let hummock = context.hummock_store().await?;
    let table = get_table_catalog_by_id(meta_client, table_id).await?;
    do_scan(table, hummock).await
}

async fn do_scan(table: TableCatalog, hummock: MonitoredStateStore<HummockStorage>) -> Result<()> {
    print_table_catalog(&table);

    println!("Rows:");
    let read_epoch = hummock.inner().get_pinned_version().max_committed_epoch();
    let storage_table = make_storage_table(hummock, &table);
    let stream = storage_table
        .batch_iter(
            HummockReadEpoch::Committed(read_epoch),
            true,
            PrefetchOptions::new_for_exhaust_iter(),
        )
        .await?;
    pin_mut!(stream);
    while let Some(item) = stream.next().await {
        println!("{:?}", item?);
    }
    Ok(())
}
