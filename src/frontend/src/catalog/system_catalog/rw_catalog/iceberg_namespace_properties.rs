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

use risingwave_common::types::Fields;
use risingwave_frontend_macro::system_catalog;

use crate::catalog::system_catalog::SysCatalogReaderImpl;
use crate::error::Result;

// JDBC/SQL catalog integration docs: https://iceberg.apache.org/docs/1.6.1/jdbc/#configurations
// `iceberg_tables definition` in iceberg java sdk https://github.com/apache/iceberg/blob/4850b622c778deb4b234880bfd7643070e0a5458/core/src/main/java/org/apache/iceberg/jdbc/JdbcUtil.java#L402-L421
// This system table is used to store the iceberg namespace properties' metadata, but currently we don't have any properties to store.

#[derive(Fields)]
#[primary_key(catalog_name, namespace, property_key)]
struct IcebergNamespaceProperties {
    pub catalog_name: String,
    pub namespace: String,
    pub property_key: String,
    pub property_value: Option<String>,
}

#[system_catalog(table, "rw_catalog.iceberg_namespace_properties")]
fn read(_reader: &SysCatalogReaderImpl) -> Result<Vec<IcebergNamespaceProperties>> {
    Ok(vec![])
}
