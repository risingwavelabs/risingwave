use anyhow::{anyhow, Result};
use futures::{pin_mut, StreamExt};
use risingwave_common::catalog::TableId;
use risingwave_frontend::catalog::TableCatalog;
use risingwave_storage::table::state_table::StateTable;
use risingwave_storage::Keyspace;

use crate::common::HummockServiceOpts;

pub async fn scan(table_id: String) -> Result<()> {
    let hummock_opts = HummockServiceOpts::from_env()?;
    let (meta, hummock) = hummock_opts.create_hummock_store().await?;
    let mvs = meta.list_materialize_view().await?;
    let mv = mvs
        .iter()
        .find(|x| x.name == table_id)
        .ok_or_else(|| anyhow!("mv not found"))?
        .clone();
    println!("{:#?}", mv);
    let table = TableCatalog::from(&mv);
    let state_table = StateTable::new(
        Keyspace::table_root(hummock, &TableId::new(mv.id)),
        table
            .columns()
            .iter()
            .map(|x| x.column_desc.clone())
            .collect(),
        table.order_desc().iter().map(|x| x.order).collect(),
        Some(table.pks.clone()),
        table.distribution_keys().to_vec(),
    );

    let stream = state_table.iter(u64::MAX).await?;
    pin_mut!(stream);
    while let Some(item) = stream.next().await {
        let item = item?;
        println!("{:?}", item);
    }
    Ok(())
}
