use std::sync::Arc;

use risingwave_common::array::column::Column;
use risingwave_common::array::{ArrayBuilderImpl, DataChunk};
use risingwave_common::error::Result;
use risingwave_common::types::Datum;
use risingwave_common::util::chunk_coalesce::DEFAULT_CHUNK_BUFFER_SIZE;

use crate::SourceColumnDesc;

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
                .zip(&mut builders)
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
