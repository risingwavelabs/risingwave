use risingwave_common::array::Row;
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::error::Result;
use risingwave_common::types::{DataType, ScalarImpl};
use risingwave_pb::catalog::Source;

use crate::model::MetadataModel;
use crate::storage::MetaStore;

pub(crate) const RW_TABLE_SOURCE_NAME: &str = "rw_table_source";

lazy_static::lazy_static! {
    pub static ref RW_TABLE_SOURCE_SCHEMA: Schema = Schema {
      fields: vec![
        Field::with_name(DataType::Int32, "id"),
        Field::with_name(DataType::Int32, "database_id"),
        Field::with_name(DataType::Int32, "schema_id"),
        Field::with_name(DataType::Varchar, "rel_name"),
      ],
    };
}

pub async fn list_table_sources<S: MetaStore>(store: &S) -> Result<Vec<Row>> {
    let sources = Source::list(store).await?;
    Ok(sources
        .iter()
        .filter(|source| source.is_table_source())
        .map(|source| {
            Row(vec![
                Some(ScalarImpl::from(source.id as i32)),
                Some(ScalarImpl::from(source.database_id as i32)),
                Some(ScalarImpl::from(source.schema_id as i32)),
                Some(ScalarImpl::from(source.get_name().to_owned())),
            ])
        })
        .collect())
}
