use risingwave_common::array::{Row, RowDeserializer};
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::error::Result;
use risingwave_common::types::{DataType, ScalarImpl};

use crate::storage::MetaStore;

/// `rw_catalog.rw_authid` catalog table, which is compatible with `pg_authid` in
/// postgresql.
///
/// See [`https://www.postgresql.org/docs/current/catalog-pg-authid.html`]
pub(crate) const RW_AUTHID_NAME: &str = "rw_authid";

lazy_static::lazy_static! {
    // Schema for `rw_catalog.rw_authid` table.
    pub static ref RW_AUTHID_SCHEMA: Schema = Schema {
        fields: vec![
            Field::with_name(DataType::Int32, "oid".into()),
            Field::with_name(DataType::Varchar, "rolname".into()),
            Field::with_name(DataType::Boolean, "rolsuper".into()),
            Field::with_name(DataType::Boolean, "rolinherit".into()),
            Field::with_name(DataType::Boolean, "rolcreaterole".into()),
            Field::with_name(DataType::Boolean, "rolcreatedb".into()),
            Field::with_name(DataType::Boolean, "rolcanlogin".into()),
            Field::with_name(DataType::Boolean, "rolreplication".into()),
            Field::with_name(DataType::Boolean, "rolbypassrls".into()),
            Field::with_name(DataType::Int32, "rolconnlimit".into()),
            Field::with_name(DataType::Varchar, "rolpassword".into()),
            Field::with_name(DataType::Timestamp, "rolvaliduntil".into()),
        ],
    };

    pub static ref RW_AUTHID_DEFAULT: Vec<Row> = vec![Row(vec![
        Some(ScalarImpl::Int32(10)),
        Some(ScalarImpl::Utf8("risingwave".into())),
        Some(ScalarImpl::Bool(true)),
        Some(ScalarImpl::Bool(true)),
        Some(ScalarImpl::Bool(true)),
        Some(ScalarImpl::Bool(true)),
        Some(ScalarImpl::Bool(true)),
        Some(ScalarImpl::Bool(true)),
        Some(ScalarImpl::Bool(true)),
        Some(ScalarImpl::Int32(-1)),
        None,
        None,
    ])];
}

pub async fn list_auth_ids<S: MetaStore>(store: &S) -> Result<Vec<Row>> {
    let mut rows = Vec::new();
    for bytes in store.list_cf("cf/rw_authid").await? {
        let deserializer = RowDeserializer::new(RW_AUTHID_SCHEMA.data_types());
        rows.push(deserializer.deserialize(&bytes)?);
    }

    Ok(rows)
}
