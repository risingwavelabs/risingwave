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

use anyhow::{Context, anyhow};
use clap::ValueEnum;
use risingwave_common::acl::AclMode;
use risingwave_common::bail;
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::config::MetaBackend;
use risingwave_common::types::DataType::Varchar;
use risingwave_common::types::Fields;
use risingwave_connector::parser::postgres_row_to_owned_row;
use risingwave_frontend_macro::system_catalog;
use risingwave_pb::user::grant_privilege::Object as GrantObject;
use thiserror_ext::AsReport;

use crate::catalog::root_catalog::SchemaPath;
use crate::catalog::system_catalog::SysCatalogReaderImpl;
use crate::error::{ErrorCode, Result};

// JDBC/SQL catalog integration docs: https://iceberg.apache.org/docs/latest/jdbc/#configurations
// `iceberg_tables` definition in iceberg java sdk https://github.com/apache/iceberg/blob/4850b622c778deb4b234880bfd7643070e0a5458/core/src/main/java/org/apache/iceberg/jdbc/JdbcUtil.java#L125-L146
// This system table is used to store the iceberg tables' metadata and only show the tables that the user has access to,
// so it can be used by other query engine to fetch iceberg catalog and provide an access control layer.

#[derive(Fields)]
#[primary_key(catalog_name, table_namespace, table_name)]
struct IcebergTables {
    pub catalog_name: String,
    pub table_namespace: String,
    pub table_name: String,
    pub metadata_location: Option<String>,
    pub previous_metadata_location: Option<String>,
    pub iceberg_type: Option<String>,
}

#[system_catalog(table, "rw_catalog.iceberg_tables")]
async fn read(reader: &SysCatalogReaderImpl) -> Result<Vec<IcebergTables>> {
    let meta_store_endpoint = reader.meta_client.get_meta_store_endpoint().await?;

    let meta_store_endpoint = url::Url::parse(&meta_store_endpoint).map_err(|_| {
        ErrorCode::InternalError("failed to parse the meta store endpoint".to_owned())
    })?;
    let meta_store_backend = meta_store_endpoint.scheme().to_owned();
    let meta_store_user = meta_store_endpoint.username().to_owned();
    let meta_store_password = meta_store_endpoint
        .password()
        .ok_or_else(|| {
            ErrorCode::InternalError("failed to parse password from meta store endpoint".to_owned())
        })?
        .to_owned();
    let meta_store_host = meta_store_endpoint
        .host_str()
        .ok_or_else(|| {
            ErrorCode::InternalError("failed to parse host from meta store endpoint".to_owned())
        })?
        .to_owned();
    let meta_store_port = meta_store_endpoint.port().ok_or_else(|| {
        ErrorCode::InternalError("failed to parse port from meta store endpoint".to_owned())
    })?;
    let meta_store_database = meta_store_endpoint
        .path()
        .trim_start_matches('/')
        .to_owned();

    let Ok(meta_backend) = MetaBackend::from_str(&meta_store_backend, true) else {
        bail!("failed to parse meta backend: {}", meta_store_backend);
    };

    if !matches!(meta_backend, MetaBackend::Postgres) {
        bail!("only support `iceberg_tables` in postgres backend");
    }

    let conn_str = format!(
        "host={} port={} user={} password={} dbname={}",
        meta_store_host, meta_store_port, meta_store_user, meta_store_password, meta_store_database
    );

    let (client, conn) = tokio_postgres::connect(&conn_str, tokio_postgres::NoTls)
        .await
        .map_err(|e| anyhow!(e))?;

    tokio::spawn(async move {
        if let Err(e) = conn.await {
            tracing::error!(
                error = ?e.as_report(),
                "iceberg_tables connection error"
            );
        }
    });

    let rows = client
        .query("select * from iceberg_tables", &[])
        .await
        .context("iceberg_tables received error from remote server")?;

    let schema = Schema::new(vec![
        Field::with_name(Varchar, "catalog_name"),
        Field::with_name(Varchar, "table_namespace"),
        Field::with_name(Varchar, "table_name"),
        Field::with_name(Varchar, "metadata_location"),
        Field::with_name(Varchar, "previous_metadata_location"),
        Field::with_name(Varchar, "iceberg_type"),
    ]);

    let catalog_reader = reader.catalog_reader.read_guard();
    let user_reader = reader.user_info_reader.read_guard();
    let user = user_reader
        .get_user_by_name(&reader.auth_context.user_name)
        .ok_or_else(|| anyhow!("User not found"))?;

    let mut res = Vec::new();
    // deserialize the rows
    for row in rows {
        let owned_row = postgres_row_to_owned_row(row, &schema);
        let record = IcebergTables {
            catalog_name: owned_row[0].clone().unwrap().as_utf8().to_string(),
            table_namespace: owned_row[1].clone().unwrap().as_utf8().to_string(),
            table_name: owned_row[2].clone().unwrap().as_utf8().to_string(),
            metadata_location: owned_row[3].clone().map(|x| x.as_utf8().to_string()),
            previous_metadata_location: owned_row[4].clone().map(|x| x.as_utf8().to_string()),
            iceberg_type: owned_row[5].clone().map(|x| x.as_utf8().to_string()),
        };
        let table = catalog_reader
            .get_created_table_by_name(
                &record.catalog_name,
                SchemaPath::Name(&record.table_namespace),
                &record.table_name,
            )?
            .0;

        if user.is_super
            || table.owner == user.id
            || user.has_privilege(
                &GrantObject::TableId(table.id().table_id()),
                AclMode::Select,
            )
        {
            res.push(record);
        }
    }

    Ok(res)
}
