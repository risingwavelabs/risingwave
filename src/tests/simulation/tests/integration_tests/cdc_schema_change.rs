// Copyright 2026 RisingWave Labs
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

use anyhow::Result;
use risingwave_common::catalog::{ColumnCatalog, ColumnDesc, ColumnId};
use risingwave_common::config::RwConfig;
use risingwave_common::types::DataType;
use risingwave_common::util::addr::HostAddr;
use risingwave_common::util::meta_addr::MetaAddressStrategy;
use risingwave_meta_model::table;
use risingwave_pb::common::WorkerType;
use risingwave_pb::common::worker_node::Property;
use risingwave_pb::ddl_service::table_schema_change::TableChangeType;
use risingwave_pb::ddl_service::{SchemaChangeEnvelope, TableSchemaChange};
use risingwave_rpc_client::MetaClient;
use risingwave_simulation::cluster::{Cluster, Configuration};
use sea_orm::sea_query::Expr;
use sea_orm::{ColumnTrait, Database, EntityTrait, QueryFilter};

#[tokio::test]
async fn test_cdc_schema_noop_without_frontend() -> Result<()> {
    let mut cluster = Cluster::start(Configuration::default()).await?;
    cluster
        .run("CREATE TABLE cdc_recovery (id INT PRIMARY KEY, payload VARCHAR)")
        .await?;

    // Seed a CDC catalog entry without an external connector, which cannot run in madsim.
    // The RPC must compare against these persisted columns even with no frontend running.
    let db = Database::connect(format!(
        "sqlite://{}?mode=rw",
        cluster.meta_sqlite_path().display()
    ))
    .await?;
    let cdc_table_id = "1.public.cdc_recovery";
    let updated = table::Entity::update_many()
        .col_expr(table::Column::CdcTableId, Expr::value(cdc_table_id))
        .filter(table::Column::Name.eq("cdc_recovery"))
        .exec(&db)
        .await?;
    assert_eq!(updated.rows_affected, 1);
    db.close().await?;
    cluster.handle().kill("frontend-1");

    cluster
        .run_on_client(async move {
            let (meta_client, _) = MetaClient::register_new(
                "http://meta-1:5690".parse::<MetaAddressStrategy>()?,
                WorkerType::RiseCtl,
                &"cdc-recovery-test:0".parse::<HostAddr>()?,
                Property::default(),
                Arc::new(RwConfig::default().meta),
            )
            .await;
            for worker in meta_client
                .list_worker_nodes(Some(WorkerType::Frontend))
                .await?
            {
                meta_client.delete_worker_node(worker.host.unwrap()).await?;
            }
            let column = |name, data_type| {
                ColumnCatalog::visible(ColumnDesc::named(name, ColumnId::placeholder(), data_type))
                    .to_protobuf()
            };
            let mut change = TableSchemaChange {
                change_type: TableChangeType::Alter as _,
                cdc_table_id: cdc_table_id.to_owned(),
                columns: vec![
                    column("id", DataType::Int32),
                    column("payload", DataType::Varchar),
                ],
                upstream_ddl: "UNKNOWN_DDL".to_owned(),
            };
            for _ in 0..2 {
                let response = meta_client
                    .auto_schema_change(SchemaChangeEnvelope {
                        table_changes: vec![change.clone()],
                    })
                    .await?;
                assert!(response.applied_cdc_table_ids.is_empty());
            }

            // A real addition must still reach replacement planning and require a frontend.
            change.columns.push(column("added", DataType::Int32));
            let error = meta_client
                .auto_schema_change(SchemaChangeEnvelope {
                    table_changes: vec![change],
                })
                .await
                .unwrap_err();
            assert!(format!("{error:?}").contains("no frontend worker available"));
            meta_client.try_unregister().await;
            Ok(())
        })
        .await
}
