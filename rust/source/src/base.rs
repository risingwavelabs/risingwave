use std::fmt::Debug;
use std::sync::Arc;

use risingwave_common::array::column::Column;
use risingwave_common::array::{ArrayBuilderImpl, DataChunk};
use risingwave_common::catalog::{ColumnDesc, ColumnId, TableId};
use risingwave_common::error::Result;
use risingwave_common::types::{DataType, Datum};
use risingwave_common::util::chunk_coalesce::DEFAULT_CHUNK_BUFFER_SIZE;
use risingwave_connector::ConnectorConfig;
use risingwave_pb::data::StreamChunk;

use crate::connector_source::ConnectorSource;
use crate::table_v2::TableSourceV2;
use crate::SourceParser;

/// `SourceColumnDesc` is used to describe a column in the Source and is used as the column
/// counterpart in `StreamScan`
#[derive(Clone, Debug)]
pub struct SourceColumnDesc {
    pub name: String,
    pub data_type: DataType,
    pub column_id: ColumnId,
    pub skip_parse: bool,
}

impl From<&ColumnDesc> for SourceColumnDesc {
    fn from(c: &ColumnDesc) -> Self {
        Self {
            name: c.name.clone(),
            data_type: c.data_type.clone(),
            column_id: c.column_id,
            skip_parse: false,
        }
    }
}

pub type SourceRef = Box<SourceImpl>;

#[derive(Debug)]
pub enum SourceImpl {
    Connector(ConnectorSource),
    TableV2(TableSourceV2),
}

impl SourceImpl {
    pub fn as_table_v2(&self) -> &TableSourceV2 {
        match self {
            SourceImpl::TableV2(table) => table,
            _ => panic!("not a table source v2"),
        }
    }
}

/// `SourceDesc` is used to describe a `Source`
#[derive(Clone, Debug)]
pub struct SourceDesc {
    pub source: SourceRef,
    pub format: SourceFormat,
    pub columns: Vec<SourceColumnDesc>,
    pub row_id_index: Option<usize>,
}

pub trait SourceManager: Debug + Sync + Send {
    fn create_source(
        &self,
        source_id: &TableId,
        format: SourceFormat,
        parser: Box<dyn SourceParser>,
        config: &ConnectorConfig,
        columns: Vec<SourceColumnDesc>,
        row_id_index: Option<usize>,
    ) -> Result<()>;
    fn create_table_source_v2(&self, table_id: &TableId, columns: Vec<ColumnDesc>) -> Result<()>;

    fn get_source(&self, source_id: &TableId) -> Result<SourceDesc>;
    fn drop_source(&self, source_id: &TableId) -> Result<()>;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SourceFormat {
    Invalid,
    Json,
    Protobuf,
    DebeziumJson,
    Avro,
}

pub(crate) trait SourceChunkBuilder {
    fn build_columns(
        column_descs: &[SourceColumnDesc],
        rows: &[Vec<Datum>],
    ) -> Result<Vec<Column>> {
        let mut builders = column_descs
            .iter()
            .map(|k| k.data_type.create_array_builder(DEFAULT_CHUNK_BUFFER_SIZE))
            .collect::<Result<Vec<ArrayBuilderImpl>>>()?;

        for row in rows {
            row.iter()
                .zip_eq(&mut builders)
                .try_for_each(|(datum, builder)| builder.append_datum(datum))?
        }

        builders
            .into_iter()
            .map(|builder| builder.finish().map(|arr| Column::new(Arc::new(arr))))
            .collect::<Result<Vec<Column>>>()
    }

    fn build_datachunk(column_desc: &[SourceColumnDesc], rows: &[Vec<Datum>]) -> Result<DataChunk> {
        let columns = Self::build_columns(column_desc, rows)?;
        Ok(DataChunk::builder().columns(columns).build())
    }
}
