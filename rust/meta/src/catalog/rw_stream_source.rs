use risingwave_common::array::Row;
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::error::Result;
use risingwave_common::types::{DataType, ScalarImpl};
use risingwave_pb::catalog::source::Info;
use risingwave_pb::catalog::Source;

use crate::model::MetadataModel;
use crate::storage::MetaStore;

pub(crate) const RW_STREAM_SOURCE_NAME: &str = "rw_stream_source";

lazy_static::lazy_static! {
    pub static ref RW_STREAM_SOURCE_SCHEMA: Schema = Schema {
      fields: vec![
        Field::with_name(DataType::Int32, "id"),
        Field::with_name(DataType::Int32, "database_id"),
        Field::with_name(DataType::Int32, "schema_id"),
        Field::with_name(DataType::Varchar, "rel_name"),
        Field::with_name(DataType::Int32, "row_format"),
        Field::with_name(DataType::Int32, "row_id_index"),
        Field::with_name(DataType::Varchar, "row_schema_location"),
        Field::with_name(DataType::Varchar, "properties")
      ],
    };
}

pub async fn list_stream_sources<S: MetaStore>(store: &S) -> Result<Vec<Row>> {
    let sources = Source::list(store).await?;
    Ok(sources
        .iter()
        .filter(|source| source.is_stream_source())
        .map(|source| {
            if let Info::StreamSource(src) = source.get_info().unwrap() {
                // TODO: extract properties.
                Row(vec![
                    Some(ScalarImpl::from(source.id as i32)),
                    Some(ScalarImpl::from(source.database_id as i32)),
                    Some(ScalarImpl::from(source.schema_id as i32)),
                    Some(ScalarImpl::from(source.get_name().to_owned())),
                    Some(ScalarImpl::from(src.row_format)),
                    Some(ScalarImpl::from(src.row_id_index)),
                    Some(ScalarImpl::from(src.get_row_schema_location().to_owned())),
                    None,
                ])
            } else {
                unreachable!()
            }
        })
        .collect())
}
