use risingwave_common::array::Row;
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::error::Result;
use risingwave_common::types::{DataType, ScalarImpl};
use risingwave_pb::meta::table::Info;
use risingwave_pb::meta::Table;

use crate::model::MetadataModel;
use crate::storage::MetaStore;

pub(crate) const RW_STREAM_SOURCE_NAME: &str = "rw_stream_source";

lazy_static::lazy_static! {
    pub static ref RW_STREAM_SOURCE_SCHEMA: Schema = Schema {
      fields: vec![
        Field::with_name(DataType::Int32, "id".into()),
        Field::with_name(DataType::Varchar, "rel_name".into()),
        Field::with_name(DataType::Boolean, "append_only".into()),
        Field::with_name(DataType::Int32, "row_format".into()),
        Field::with_name(DataType::Varchar, "row_schema_location".into()),
        Field::with_name(DataType::Varchar, "properties".into())
      ],
    };
}

pub async fn list_stream_sources<S: MetaStore>(store: &S) -> Result<Vec<Row>> {
    let tables = Table::list(store).await?;
    Ok(tables
        .iter()
        .filter(|table| table.is_stream_source())
        .map(|table| {
            if let Info::StreamSource(src) = table.get_info().unwrap() {
                // TODO: extract properties.
                Row(vec![
                    Some(ScalarImpl::from(table.get_table_ref_id().unwrap().table_id)),
                    Some(ScalarImpl::from(table.get_table_name().to_owned())),
                    Some(ScalarImpl::from(src.append_only)),
                    Some(ScalarImpl::from(src.row_format)),
                    Some(ScalarImpl::from(src.get_row_schema_location().to_owned())),
                    None,
                ])
            } else {
                unreachable!()
            }
        })
        .collect())
}
