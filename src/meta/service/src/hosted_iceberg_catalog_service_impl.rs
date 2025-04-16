// Copyright 2025 RisingWave Labs
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

use risingwave_meta::manager::MetaSrvEnv;
use risingwave_pb::meta::hosted_iceberg_catalog_service_server::HostedIcebergCatalogService;
use risingwave_pb::meta::{
    ListIcebergTablesRequest, ListIcebergTablesResponse, list_iceberg_tables_response,
};
use sea_orm::{ConnectionTrait, DeriveIden, EnumIter, Statement, TryGetableMany};
use tonic::{Request, Response, Status};

#[derive(Clone)]
pub struct HostedIcebergCatalogServiceImpl {
    env: MetaSrvEnv,
}

impl HostedIcebergCatalogServiceImpl {
    pub fn new(env: MetaSrvEnv) -> Self {
        HostedIcebergCatalogServiceImpl { env }
    }
}

#[derive(EnumIter, DeriveIden)]
enum IcebergTableCols {
    CatalogName,
    TableNamespace,
    TableName,
    MetadataLocation,
    PreviousMetadataLocation,
}

#[async_trait::async_trait]
impl HostedIcebergCatalogService for HostedIcebergCatalogServiceImpl {
    #[cfg_attr(coverage, coverage(off))]
    async fn list_iceberg_tables(
        &self,
        _request: Request<ListIcebergTablesRequest>,
    ) -> Result<Response<ListIcebergTablesResponse>, Status> {
        let rows: Vec<(String, String, String, Option<String>, Option<String>)> =
            <(String, String, String, Option<String>, Option<String>)>::find_by_statement::<IcebergTableCols>(Statement::from_sql_and_values(
                self.env.meta_store().conn.get_database_backend(),
                r#"SELECT catalog_name, table_namespace, table_name, metadata_location, previous_metadata_location FROM iceberg_tables"#,
                [],
            ))
                .all(&self.env.meta_store().conn)
                .await.map_err(|e| Status::internal(format!("Failed to list iceberg tables: {}", e)))?;

        let iceberg_tables = rows
            .into_iter()
            .map(|row| list_iceberg_tables_response::IcebergTable {
                catalog_name: row.0,
                table_namespace: row.1,
                table_name: row.2,
                metadata_location: row.3,
                previous_metadata_location: row.4,
                iceberg_type: None,
            })
            .collect();

        return Ok(Response::new(ListIcebergTablesResponse { iceberg_tables }));
    }
}
