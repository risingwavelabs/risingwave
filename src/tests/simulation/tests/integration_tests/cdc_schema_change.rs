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
use risingwave_pb::ddl_service::auto_schema_change_result::Outcome;
use risingwave_pb::ddl_service::table_schema_change::TableChangeType;
use risingwave_pb::ddl_service::{SchemaChangeEnvelope, TableSchemaChange};
use risingwave_rpc_client::MetaClient;
use risingwave_simulation::cluster::{Cluster, Configuration};
use sea_orm::sea_query::Expr;
use sea_orm::{ColumnTrait, Database, EntityTrait, QueryFilter};

#[tokio::test]
async fn test_cdc_schema_change_outcomes() -> Result<()> {
    let mut cluster = Cluster::start(Configuration::default()).await?;
    let names = ["cdc_noop", "cdc_applied", "cdc_failed"];
    for name in names {
        cluster
            .run(format!(
                "CREATE TABLE {name} (id INT PRIMARY KEY, payload VARCHAR)"
            ))
            .await?;
    }

    // Associate ordinary test tables with CDC IDs to exercise the Meta/Frontend RPC path
    // without running an external CDC connector, which is unavailable in madsim.
    let db = Database::connect(format!(
        "sqlite://{}?mode=rw",
        cluster.meta_sqlite_path().display()
    ))
    .await?;
    let mut table_ids = Vec::new();
    for name in names {
        let table = table::Entity::find()
            .filter(table::Column::Name.eq(name))
            .one(&db)
            .await?
            .unwrap();
        table_ids.push(table.table_id);
        let updated = table::Entity::update_many()
            .col_expr(
                table::Column::CdcTableId,
                Expr::value(format!("1.public.{name}")),
            )
            .filter(table::Column::TableId.eq(table.table_id))
            .exec(&db)
            .await?;
        assert_eq!(updated.rows_affected, 1);
    }
    db.close().await?;

    cluster
        .run_on_client(async move {
            let (meta_client, _) = MetaClient::register_new(
                "http://meta-1:5690".parse::<MetaAddressStrategy>()?,
                WorkerType::RiseCtl,
                &"cdc-outcomes-test:0".parse::<HostAddr>()?,
                Property::default(),
                Arc::new(RwConfig::default().meta),
            )
            .await;
            let column = |name, data_type| {
                ColumnCatalog::visible(ColumnDesc::named(name, ColumnId::placeholder(), data_type))
                    .to_protobuf()
            };
            let change = |name, columns| TableSchemaChange {
                change_type: TableChangeType::Alter as _,
                cdc_table_id: format!("1.public.{name}"),
                columns,
                upstream_ddl: "UNKNOWN_DDL".to_owned(),
            };
            let columns = vec![
                column("id", DataType::Int32),
                column("payload", DataType::Varchar),
            ];
            let mut added_columns = columns.clone();
            added_columns.push(column("added", DataType::Int32));
            let response = meta_client
                .auto_schema_change(SchemaChangeEnvelope {
                    table_changes: vec![
                        change("cdc_noop", columns.clone()),
                        change("cdc_applied", added_columns),
                        // Dropping the primary key passes the subset check but fails planning.
                        change("cdc_failed", vec![column("payload", DataType::Varchar)]),
                        change("cdc_missing", columns.clone()),
                    ],
                })
                .await?;
            let outcomes: Vec<_> = response
                .results
                .iter()
                .map(|result| {
                    (
                        result.cdc_table_id.as_str(),
                        result.table_id,
                        result.outcome(),
                    )
                })
                .collect();
            assert_eq!(
                outcomes,
                vec![
                    ("1.public.cdc_noop", Some(table_ids[0]), Outcome::Noop),
                    ("1.public.cdc_applied", Some(table_ids[1]), Outcome::Applied),
                    ("1.public.cdc_failed", Some(table_ids[2]), Outcome::Failed),
                    ("1.public.cdc_missing", None, Outcome::Noop),
                ]
            );

            let response = meta_client
                .auto_schema_change(SchemaChangeEnvelope {
                    table_changes: vec![change("cdc_noop", columns)],
                })
                .await?;
            assert_eq!(response.results.len(), 1);
            assert_eq!(response.results[0].outcome(), Outcome::Noop);
            meta_client.try_unregister().await;
            anyhow::Ok(())
        })
        .await?;

    cluster
        .run("INSERT INTO cdc_applied VALUES (1, 'after', 7)")
        .await?;
    cluster.run("FLUSH").await?;
    assert_eq!(cluster.run("SELECT added FROM cdc_applied").await?, "7");
    cluster
        .run("INSERT INTO cdc_failed VALUES (1, 'unchanged')")
        .await?;
    Ok(())
}
