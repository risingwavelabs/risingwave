
use std::ops::Deref;


use risingwave_common::array::{Row, RowDeserializer};
use risingwave_common::catalog::{Schema};
use risingwave_common::error::Result;



use crate::catalog::rw_auth_members::*;
use crate::catalog::rw_authid::*;

use crate::storage::MetaStore;

mod rw_auth_members;
mod rw_authid;

#[async_trait::async_trait]
pub trait CatalogTable {
    /// Table name.
    fn name() -> &'static str;
    /// Schema of the table.
    fn schema() -> &'static Schema;
    /// Initialize the table with default records.
    async fn init<S: MetaStore>(store: &S) -> Result<()>;
    /// List all records in the table.
    async fn list<S: MetaStore>(store: &S) -> Result<Vec<Row>>;
}

// macro to define a new system catalog, with a name and a list of columns.
macro_rules! catalog_table_impl {
    ([], $( { $table:ident, $name:ident, $schema:ident, $default_records:ident } ),*) => {
        $(
            pub struct $table;

            #[async_trait::async_trait]
            impl CatalogTable for $table {
                fn name() -> &'static str {
                    $name
                }

                fn schema() -> &'static Schema {
                    &$schema
                }

                async fn init<S: MetaStore>(store: &S) -> Result<()> {
                    for row in $default_records.deref() {
                        let bytes = row.serialize()?;
                        store
                            .put_cf(
                                format!("cf/{}", $name).as_str(),
                                row.serialize_datum(0)?,
                                bytes,
                            )
                            .await?;
                    }

                    Ok(())
                }

                async fn list<S: MetaStore>(store: &S) -> Result<Vec<Row>> {
                    let mut rows = Vec::new();
                    for bytes in store.list_cf(format!("cf/{}", $name).as_str()).await? {
                        let deserializer = RowDeserializer::new($schema.data_types());
                        rows.push(deserializer.deserialize(&bytes)?);
                    }

                    Ok(rows)
                }
            }
        )*
    };
}

/// `for_all_catalog_table_impl` includes all system catalogs. If you added a new system catalog, be
/// sure to add a corresponding entry here.
///
/// Every tuple has four elements:
/// `{table struct, table name, table schema, default records}`
macro_rules! for_all_catalog_table_impl {
    ($macro:tt $(, $x:tt)*) => {
        $macro! {
            [$($x),*],
            {RwAuthMembers, RW_AUTH_MEMBERS_NAME, RW_AUTH_MEMBERS_SCHEMA, RW_AUTH_MEMBERS_DEFAULT},
            {RwAuthId, RW_AUTHID_NAME, RW_AUTHID_SCHEMA, RW_AUTHID_DEFAULT}
        }
    };
}

for_all_catalog_table_impl! { catalog_table_impl }

// pub struct SystemCatalogSrv<S> {
//     catalogs: RwLock<HashMap<TableId, Box<dyn CatalogTable>>>,
// }
//
// const SYSTEM_CATALOG_DATABASE_ID: i32 = 1;
// const SYSTEM_CATALOG_SCHEMA_ID: i32 = 1;
//
// impl<S> SystemCatalogSrv<S>
// where
//     S: MetaStore,
// {
//     pub async fn init(&self, store: &S) -> Result<Self> {
//         macro_rules! catalog_table_init {
//             () => {};
//         }
//
//         let database = Database {
//             database_ref_id: Some(DatabaseRefId {
//                 database_id: SYSTEM_CATALOG_DATABASE_ID,
//             }),
//             database_name: "rw_catalog".to_string(),
//             ..Default::default()
//         };
//         database.insert(store).await?;
//
//         let schema = ProstSchema {
//             schema_ref_id: Some(SchemaRefId {
//                 database_ref_id: database.database_ref_id,
//                 schema_id: SYSTEM_CATALOG_SCHEMA_ID,
//             }),
//             schema_name: "rw_catalog".to_string(),
//             ..Default::default()
//         };
//         schema.insert(store).await?;
//
//         for_all_catalog_table_impl! { catalog_table_init };
//
//         Ok(SystemCatalogSrv {
//             catalogs: Default::default(),
//         })
//     }
//
//     pub fn get_table(&self, table_id: &TableId) -> Option<Box<dyn CatalogTable>> {
//         let guard = self.catalogs.read();
//         guard.get(table_id).cloned()
//     }
// }

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manager::MetaSrvEnv;

    #[tokio::test]
    async fn test_catalog_table_impl() -> Result<()> {
        let store = &MetaSrvEnv::for_test().await.meta_store_ref();
        RwAuthMembers::init(&**store).await?;
        let rows = RwAuthMembers::list(&**store).await?;
        assert_eq!(rows, *RW_AUTH_MEMBERS_DEFAULT.deref());

        Ok(())
    }
}
