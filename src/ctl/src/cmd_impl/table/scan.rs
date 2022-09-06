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

use anyhow::{anyhow, Result};
use futures::{pin_mut, StreamExt};
use risingwave_frontend::TableCatalog;
use risingwave_rpc_client::MetaClient;
use risingwave_storage::hummock::HummockStorage;
use risingwave_storage::monitor::MonitoredStateStore;
use risingwave_storage::table::streaming_table::state_table::StateTable;
use risingwave_storage::table::Distribution;
use risingwave_storage::StateStore;

use crate::common::HummockServiceOpts;

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

pub fn make_state_table<S: StateStore>(hummock: S, table: &TableCatalog) -> StateTable<S> {
    StateTable::new_with_distribution(
        hummock,
        table.id,
        table
            .columns()
            .iter()
            .map(|x| x.column_desc.clone())
            .collect(),
        table
            .order_key()
            .iter()
            .map(|x| x.direct.to_order())
            .collect(),
        table.stream_key.clone(), // FIXME: should use order keys
        Distribution::all_vnodes(table.distribution_key().to_vec()), // scan all vnodes
    )
}

pub async fn scan(mv_name: String) -> Result<()> {
    let mut hummock_opts = HummockServiceOpts::from_env()?;
    let (meta, hummock) = hummock_opts.create_hummock_store().await?;
    let table = get_table_catalog(meta.clone(), mv_name).await?;
    do_scan(table, hummock, hummock_opts).await
}

pub async fn scan_id(table_id: u32) -> Result<()> {
    let mut hummock_opts = HummockServiceOpts::from_env()?;
    let (meta, hummock) = hummock_opts.create_hummock_store().await?;
    let table = get_table_catalog_by_id(meta.clone(), table_id).await?;
    do_scan(table, hummock, hummock_opts).await
}

async fn do_scan(
    table: TableCatalog,
    hummock: MonitoredStateStore<HummockStorage>,
    mut hummock_opts: HummockServiceOpts,
) -> Result<()> {
    print_table_catalog(&table);

    // We use state table here instead of cell-based table to support iterating with u64::MAX epoch.
    let state_table = make_state_table(hummock.clone(), &table);
    let stream = state_table.iter(u64::MAX).await?;

    pin_mut!(stream);
    while let Some(item) = stream.next().await {
        println!("{:?}", item?);
    }

    hummock_opts.shutdown().await;
    Ok(())
}
