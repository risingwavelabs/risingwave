use risingwave_common::array::{Row, RowDeserializer};
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::error::Result;
use risingwave_common::types::{DataType, ScalarImpl};

use crate::storage::MetaStore;

/// `rw_catalog.rw_auth_members` catalog table, which is compatible with `pg_auth_members` in
/// postgresql.
///
/// See [`https://www.postgresql.org/docs/current/catalog-pg-auth-members.html`]
pub(crate) const RW_AUTH_MEMBERS_NAME: &str = "rw_auth_members";

lazy_static::lazy_static! {
    /// Schema for `rw_catalog.rw_auth_members` table.
    /// * role_id(references rw_authid.oid): ID of a role that has a member
    /// * member(references pg_authid.oid): ID of a role that is a member of roleid
    /// * grantor(references pg_authid.oid): ID of the role that granted this membership
    /// * admin_option: True if member can grant membership in roleid to others
    pub static ref RW_AUTH_MEMBERS_SCHEMA: Schema = Schema {
        fields: vec![
            Field::with_name(DataType::Int32, "role_id".into()),
            Field::with_name(DataType::Int32, "member".into()),
            Field::with_name(DataType::Int32, "grantor".into()),
            Field::with_name(DataType::Boolean, "admin_option".into()),
        ],
    };

    /// Default records in `rw_catalog.rw_auth_member` table.
    pub static ref RW_AUTH_MEMBERS_DEFAULT: Vec<Row> = vec![Row(vec![
        Some(ScalarImpl::Int32(1)),
        Some(ScalarImpl::Int32(3373)),
        Some(ScalarImpl::Int32(10)),
        Some(ScalarImpl::Bool(false)),
    ])];
}

pub async fn list_auth_members<S: MetaStore>(store: &S) -> Result<Vec<Row>> {
    let mut rows = Vec::new();
    for bytes in store.list_cf("cf/rw_auth_members").await? {
        let deserializer = RowDeserializer::new(RW_AUTH_MEMBERS_SCHEMA.data_types());
        rows.push(deserializer.deserialize(&bytes)?);
    }

    Ok(rows)
}
