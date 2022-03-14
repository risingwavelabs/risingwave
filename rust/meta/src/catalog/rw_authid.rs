use risingwave_common::array::{Row, RowDeserializer};
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::error::Result;
use risingwave_common::types::{DataType, ScalarImpl};

use crate::storage::MetaStore;

pub(crate) const RW_AUTH_NAME: &str = "rw_auth";
/// Column family name for auth virtual table.
const CATALOG_SOURCE_CF_NAME: &str = "cf/virtual_table_rw_auth";

lazy_static::lazy_static! {
    // Schema for `rw_catalog.rw_auth` table.
    pub static ref RW_AUTH_SCHEMA: Schema = Schema {
        fields: vec![
            Field::with_name(DataType::Varchar, "user_name"),
            Field::with_name(DataType::Boolean, "is_super"),
            Field::with_name(DataType::Boolean, "can_create_db"),
            Field::with_name(DataType::Boolean, "can_login"),
            Field::with_name(DataType::Varchar, "rol_password"),
        ],
    };
}

pub async fn list_auth_info<S: MetaStore>(store: &S) -> Result<Vec<Row>> {
    let bytes_vec = store.list_cf(CATALOG_SOURCE_CF_NAME).await?;
    if !bytes_vec.is_empty() {
        let mut rows = Vec::new();
        for bytes in bytes_vec {
            let deserializer = RowDeserializer::new(RW_AUTH_SCHEMA.data_types());
            rows.push(deserializer.deserialize(&bytes)?);
        }
        return Ok(rows);
    }

    // A workaround to initialize the auth table with default records.
    let row = Row::new(vec![
        Some(ScalarImpl::Utf8("risingwave".into())),
        Some(ScalarImpl::Bool(true)),
        Some(ScalarImpl::Bool(true)),
        Some(ScalarImpl::Bool(true)),
        None,
    ]);
    store
        .put_cf(
            CATALOG_SOURCE_CF_NAME,
            b"risingwave".to_vec(),
            row.serialize()?,
        )
        .await?;

    Ok(vec![row])
}
