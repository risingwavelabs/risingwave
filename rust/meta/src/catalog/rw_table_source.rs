use risingwave_common::array::Row;
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::error::Result;
use risingwave_common::types::{DataType, ScalarImpl};
use risingwave_pb::meta::Table;

use crate::model::MetadataModel;
use crate::storage::MetaStore;

pub(crate) const RW_TABLE_SOURCE_NAME: &str = "rw_table_source";

lazy_static::lazy_static! {
    pub static ref RW_TABLE_SOURCE_SCHEMA: Schema = Schema {
      fields: vec![
        Field::with_name(DataType::Int32, "id".into()),
        Field::with_name(DataType::Varchar, "rel_name".into()),
      ],
    };
}

pub async fn list_table_sources<S: MetaStore>(store: &S) -> Result<Vec<Row>> {
    let tables = Table::list(store).await?;
    Ok(tables
        .iter()
        .filter(|table| table.is_table_source())
        .map(|table| {
            Row(vec![
                Some(ScalarImpl::from(table.get_table_ref_id().unwrap().table_id)),
                Some(ScalarImpl::from(table.get_table_name().to_owned())),
            ])
        })
        .collect())
}
