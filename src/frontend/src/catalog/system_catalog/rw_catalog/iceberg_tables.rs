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

use anyhow::anyhow;
use risingwave_common::acl::AclMode;
use risingwave_common::types::Fields;
use risingwave_frontend_macro::system_catalog;
use risingwave_pb::user::grant_privilege::Object as GrantObject;

use crate::catalog::root_catalog::SchemaPath;
use crate::catalog::system_catalog::SysCatalogReaderImpl;
use crate::error::Result;

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
    let rows = reader.meta_client.list_hosted_iceberg_tables().await?;

    let catalog_reader = reader.catalog_reader.read_guard();
    let user_reader = reader.user_info_reader.read_guard();
    let user = user_reader
        .get_user_by_name(&reader.auth_context.user_name)
        .ok_or_else(|| anyhow!("User not found"))?;

    let mut res = Vec::new();
    for row in rows {
        let record = IcebergTables {
            catalog_name: row.catalog_name,
            table_namespace: row.table_namespace,
            table_name: row.table_name,
            metadata_location: row.metadata_location,
            previous_metadata_location: row.previous_metadata_location,
            iceberg_type: row.iceberg_type,
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
