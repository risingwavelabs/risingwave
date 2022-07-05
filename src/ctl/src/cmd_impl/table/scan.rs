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
use risingwave_frontend::catalog::TableCatalog;
use risingwave_rpc_client::MetaClient;
use risingwave_storage::table::state_table::StateTable;
use risingwave_storage::table::Distribution;
use risingwave_storage::StateStore;

use crate::common::HummockServiceOpts;

pub async fn get_table_catalog(meta: MetaClient, table_id: String) -> Result<TableCatalog> {
    let mvs = meta.list_materialize_view().await?;
    let mv = mvs
        .iter()
        .find(|x| x.name == table_id)
        .ok_or_else(|| anyhow!("mv not found"))?
        .clone();
    Ok(TableCatalog::from(&mv))
}

pub fn print_table_catalog(table: &TableCatalog) {
    let mut catalog = table.clone();
    catalog.vnode_mapping = None;
    println!("{:#?}", catalog);
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
        table.order_desc().iter().map(|x| x.order).collect(),
        table.pks.clone(), // FIXME: should use order keys
        Distribution::all_vnodes(table.distribution_keys().to_vec()), // scan all vnodes
    )
}

pub async fn scan(table_id: String) -> Result<()> {
    let mut hummock_opts = HummockServiceOpts::from_env()?;
    let (meta, hummock) = hummock_opts.create_hummock_store().await?;
    let table = get_table_catalog(meta.clone(), table_id).await?;
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
