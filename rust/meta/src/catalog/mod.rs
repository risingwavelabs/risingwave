#![allow(dead_code)]
use std::collections::HashMap;
use std::sync::RwLock;

use risingwave_common::array::Row;
use risingwave_common::catalog::{Schema, TableId};
use risingwave_common::error::Result;
use risingwave_pb::meta::Table;
use risingwave_pb::plan::{ColumnDesc, DatabaseRefId, SchemaRefId, TableRefId};

use crate::catalog::rw_auth_members::*;
use crate::catalog::rw_authid::*;
use crate::catalog::rw_materialized_view::*;
use crate::catalog::rw_stream_source::*;
use crate::catalog::rw_table_source::*;
use crate::storage::MetaStore;

mod rw_auth_members;
mod rw_authid;
mod rw_materialized_view;
mod rw_stream_source;
mod rw_table_source;

/// `for_all_catalog_table_impl` includes all system catalogs. If you added a new system catalog, be
/// sure to add a corresponding entry here.
///
/// Every tuple has four elements:
/// `{table, table id, table name, table schema}`
macro_rules! for_all_catalog_table_impl {
    ($macro:tt $(, $x:tt)*) => {
        $macro! {
            [$($x),*],
            {AuthMembers, 1, RW_AUTH_MEMBERS_NAME, RW_AUTH_MEMBERS_SCHEMA, list_auth_members},
            {AuthId, 2, RW_AUTHID_NAME, RW_AUTHID_SCHEMA, list_auth_ids},
            {StreamSource, 3, RW_STREAM_SOURCE_NAME, RW_STREAM_SOURCE_SCHEMA, list_stream_sources},
            {TableSource, 4, RW_TABLE_SOURCE_NAME, RW_TABLE_SOURCE_SCHEMA, list_table_sources},
            {MaterializedView, 5, RW_MATERIALIZED_VIEW_NAME, RW_MATERIALIZED_VIEW_SCHEMA, list_materialized_views}
        }
    };
}

/// Defines `CatalogTable` with macro.
macro_rules! catalog_table_impl {
    ([], $( { $table:ident, $id:expr, $name:ident, $schema:ident, $list_fn:ident } ),*) => {
        #[derive(Debug, Clone, PartialEq)]
        pub enum RwCatalogTable {
            $( $table ),*
        }
    };
}

const SYSTEM_CATALOG_DATABASE_ID: i32 = 1;
const SYSTEM_CATALOG_SCHEMA_ID: i32 = 1;

for_all_catalog_table_impl! { catalog_table_impl }

macro_rules! impl_catalog_func {
    ([], $( { $table:ident, $id:expr, $name:ident, $schema:ident, $list_fn:ident } ),*) => {
        impl RwCatalogTable {
            /// Returns the id of the table.
            pub fn table_id(&self) -> TableId {
                match self {
                    $( Self::$table => TableId {
                        table_id: $id,
                    }, )*
                }
            }

            /// Returns the name of the table.
            pub fn name(&self) -> &'static str {
                match self {
                    $( Self::$table => &$name, )*
                }
            }

            /// Returns the schema of the table.
            pub fn schema(&self) -> &'static Schema {
                match self {
                    $( Self::$table => &$schema, )*
                }
            }

            /// Returns table proto.
            pub fn catalog(&self) -> Table {
                match self {
                    $( Self::$table => {
                            let column_descs = $schema
                                .fields
                                .iter()
                                .enumerate()
                                .map(|(id, field)| ColumnDesc {
                                    column_type: Some(field.data_type.to_protobuf().unwrap()),
                                    column_id: id as i32,
                                    name: field.name.clone(),
                                })
                                .collect();

                            Table {
                                table_ref_id: Some(TableRefId {
                                    schema_ref_id: Some(SchemaRefId {
                                        database_ref_id: Some(DatabaseRefId {
                                            database_id: SYSTEM_CATALOG_DATABASE_ID,
                                        }),
                                        schema_id: SYSTEM_CATALOG_SCHEMA_ID,
                                    }),
                                    table_id: $id,
                                }),
                                table_name: $name.to_string(),
                                column_descs,
                                ..Default::default()
                            }
                    }, )*
                }
            }

            /// Returns the list of all rows in the table.
            pub async fn list<S: MetaStore>(&self, store: &S) -> Result<Vec<Row>> {
                match self {
                    $( Self::$table => $list_fn(store).await, )*
                }
            }
        }
    }
}

for_all_catalog_table_impl! { impl_catalog_func }

/// Defines `SystemCatalogSrv` to serve as system catalogs service.
pub struct SystemCatalogSrv {
    catalogs: RwLock<HashMap<TableId, RwCatalogTable>>,
}

impl SystemCatalogSrv {
    pub fn new() -> Self {
        let mut catalogs = HashMap::new();
        macro_rules! init_catalog_mapping {
            ([], $( { $table:ident, $id:expr, $name:ident, $schema:ident, $list_fn:ident } ),*) => {
                $(
                    catalogs.insert(
                        TableId {
                            table_id: $id,
                        },
                        RwCatalogTable::$table,
                    );
                )*
            }
        }

        for_all_catalog_table_impl! { init_catalog_mapping }

        Self {
            catalogs: RwLock::new(catalogs),
        }
    }

    pub fn get_table(&self, table_id: &TableId) -> Option<RwCatalogTable> {
        let guard = self.catalogs.read().unwrap();
        guard.get(table_id).cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manager::MetaSrvEnv;

    #[tokio::test]
    async fn test_catalog_table_impl() -> Result<()> {
        let store = MetaSrvEnv::for_test().await.meta_store_ref();
        assert_eq!(RwCatalogTable::AuthMembers.table_id().table_id, 1);
        assert_eq!(RwCatalogTable::AuthMembers.name(), RW_AUTH_MEMBERS_NAME);
        assert_eq!(
            RwCatalogTable::AuthMembers.schema().fields,
            RW_AUTH_MEMBERS_SCHEMA.fields()
        );
        assert!(RwCatalogTable::AuthMembers.list(&*store).await?.is_empty());

        assert_eq!(RwCatalogTable::AuthId.table_id().table_id, 2);
        assert_eq!(RwCatalogTable::AuthId.name(), RW_AUTHID_NAME);
        assert_eq!(
            RwCatalogTable::AuthId.schema().fields,
            RW_AUTHID_SCHEMA.fields
        );
        assert!(RwCatalogTable::AuthId.list(&*store).await?.is_empty());

        assert_eq!(RwCatalogTable::StreamSource.table_id().table_id, 3);
        assert_eq!(RwCatalogTable::StreamSource.name(), RW_STREAM_SOURCE_NAME);
        assert_eq!(
            RwCatalogTable::StreamSource.schema().fields,
            RW_STREAM_SOURCE_SCHEMA.fields
        );
        assert_eq!(
            RwCatalogTable::StreamSource.catalog().table_name,
            RW_STREAM_SOURCE_NAME
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_system_catalog_srv() -> Result<()> {
        let catalog_srv = SystemCatalogSrv::new();
        assert_eq!(
            catalog_srv.get_table(&TableId { table_id: 1 }),
            Some(RwCatalogTable::AuthMembers)
        );
        assert_eq!(
            catalog_srv.get_table(&TableId { table_id: 2 }),
            Some(RwCatalogTable::AuthId)
        );
        assert_eq!(
            catalog_srv.get_table(&TableId { table_id: 3 }),
            Some(RwCatalogTable::StreamSource)
        );

        Ok(())
    }
}
