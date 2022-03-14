use risingwave_common::array::Row;
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::error::Result;
use risingwave_common::types::{DataType, ScalarImpl};
use risingwave_pb::catalog::Table;

use crate::model::MetadataModel;
use crate::storage::MetaStore;

pub(crate) const RW_MATERIALIZED_VIEW_NAME: &str = "rw_materialized_view";

lazy_static::lazy_static! {
    pub static ref RW_MATERIALIZED_VIEW_SCHEMA: Schema = Schema {
      fields: vec![
        Field::with_name(DataType::Int32, "id"),
        Field::with_name(DataType::Int32, "database_id"),
        Field::with_name(DataType::Int32, "schema_id"),
        Field::with_name(DataType::Varchar, "rel_name"),
      ],
    };
}

pub async fn list_materialized_views<S: MetaStore>(store: &S) -> Result<Vec<Row>> {
    let tables = Table::list(store).await?;
    Ok(tables
        .iter()
        .map(|table| {
            Row(vec![
                Some(ScalarImpl::from(table.id as i32)),
                Some(ScalarImpl::from(table.database_id as i32)),
                Some(ScalarImpl::from(table.schema_id as i32)),
                Some(ScalarImpl::from(table.get_name().to_owned())),
            ])
        })
        .collect())
}
