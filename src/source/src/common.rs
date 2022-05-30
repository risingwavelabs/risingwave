// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use itertools::Itertools;
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
        Ok(DataChunk::tai(columns, rows.len(), None))
    }
}
