use risingwave_common::array::{Row, RowDeserializer};
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::error::Result;
use risingwave_common::types::{DataType, ScalarImpl};

use crate::storage::MetaStore;

pub(crate) const RW_AUTH_NAME: &str = "rw_auth";

lazy_static::lazy_static! {
    // Schema for `rw_catalog.rw_auth` table.
    pub static ref RW_AUTH_SCHEMA: Schema = Schema {
        fields: vec![
            Field::with_name(DataType::Varchar, "user_name".into()),
            Field::with_name(DataType::Boolean, "is_super".into()),
            Field::with_name(DataType::Boolean, "can_create_db".into()),
            Field::with_name(DataType::Boolean, "can_login".into()),
            Field::with_name(DataType::Varchar, "rol_password".into()),
        ],
    };
}

// TODO: try insert default auth information when init cluster.
pub async fn list_auth_info<S: MetaStore>(_store: &S) -> Result<Vec<Row>> {
    Ok(vec![Row::new(vec![
        Some(ScalarImpl::Utf8("risingwave".into())),
        Some(ScalarImpl::Boolean(true)),
        Some(ScalarImpl::Boolean(true)),
        Some(ScalarImpl::Boolean(true)),
        None
    ])])
}
