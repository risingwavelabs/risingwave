// Copyright 2024 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;
use std::sync::Arc;

use itertools::Itertools;
use parquet::arrow::arrow_reader::ParquetRecordBatchReader;
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use parquet::file::reader::{FileReader, SerializedFileReader};

use super::{ByteStreamSourceParser, ParserFormat, SourceStreamChunkRowWriter};
use crate::error::ConnectorResult;
use crate::only_parse_payload;
use crate::source::{SourceColumnDesc, SourceContext, SourceContextRef};

/// Parser for PARQUET format
#[derive(Debug)]
pub struct ParquetParser {
    rw_columns: Vec<SourceColumnDesc>,
    source_ctx: SourceContextRef,
}

impl ParquetParser {
    pub fn new(
        rw_columns: Vec<SourceColumnDesc>,
        source_ctx: SourceContextRef,
    ) -> ConnectorResult<Self> {
        Ok(Self {
            rw_columns,
            source_ctx,
        })
    }

    fn read_row(&self, buf: Vec<u8>) -> ConnectorResult<Vec<String>> {
        let payload_bytes = bytes::Bytes::from(buf);
        let reader = SerializedFileReader::new(payload_bytes).unwrap();

        let file_metadata = reader.metadata();
        let parquet_columns_name = file_metadata
            .file_metadata()
            .schema()
            .get_fields()
            .iter()
            .map(|f| f.name().to_string())
            .collect_vec();
        // let parquet_schema = file_metadata.file_metadata().schema_descr().name();
        let valid_column_index: Vec<Option<usize>> = self
            .rw_columns
            .iter()
            .map(|column_desc| {
                parquet_columns_name
                    .iter()
                    .position(|parquet_column_name| &column_desc.name == parquet_column_name)
            })
            .collect();

        todo!()
    }

    #[allow(clippy::unused_async)]
    pub async fn parse_inner(
        &mut self,
        payload: Vec<u8>,
        mut writer: SourceStreamChunkRowWriter<'_>,
    ) -> ConnectorResult<()> {
        // let mut stream = ParquetRecordBatchStreamBuilder::new(file_reader)
        // .await?
        // .with_batch_size(self.batch_size)
        // .with_projection(self.projection_mask)
        // .build()?
        // .map(|res: std::result::Result<RecordBatch, ParquetError>| res.map_err(|e| e.into()));
        let payload_bytes = bytes::Bytes::from(payload);
        let reader = SerializedFileReader::new(payload_bytes).unwrap();
        // let parquet_reader = FileReader::new(Arc::new(file_reader));
        let num_row_groups = reader.num_row_groups();
        for i in 0..num_row_groups {
            let row_group_reader = reader.get_row_group(i).unwrap();
            let num_columns = row_group_reader.num_columns();

            for j in 0..num_columns {
                let mut column_reader = row_group_reader.get_column_reader(j).unwrap();
                match column_reader {
                    parquet::column::reader::ColumnReader::BoolColumnReader(a) => {
                        let value = bytes::Bytes::new();
                        a.read_records(10, None, None, value).unwrap();
                    }
                    parquet::column::reader::ColumnReader::Int32ColumnReader(_) => todo!(),
                    parquet::column::reader::ColumnReader::Int64ColumnReader(_) => todo!(),
                    parquet::column::reader::ColumnReader::Int96ColumnReader(_) => todo!(),
                    parquet::column::reader::ColumnReader::FloatColumnReader(_) => todo!(),
                    parquet::column::reader::ColumnReader::DoubleColumnReader(_) => todo!(),
                    parquet::column::reader::ColumnReader::ByteArrayColumnReader(_) => todo!(),
                    parquet::column::reader::ColumnReader::FixedLenByteArrayColumnReader(_) => {
                        todo!()
                    }
                }
            }
        }
        let parquet_metadata = reader.metadata();
        let mut row_iter = reader
            .get_row_iter(Some(parquet_metadata.file_metadata().schema().clone()))
            .unwrap();
        while let Some(row) = row_iter.next() {
            let row = row.unwrap();
        }

        todo!()
    }
}

impl ByteStreamSourceParser for ParquetParser {
    fn columns(&self) -> &[SourceColumnDesc] {
        &self.rw_columns
    }

    fn source_ctx(&self) -> &SourceContext {
        &self.source_ctx
    }

    fn parser_format(&self) -> ParserFormat {
        ParserFormat::Parquet
    }

    async fn parse_one<'a>(
        &'a mut self,
        _key: Option<Vec<u8>>,
        payload: Option<Vec<u8>>,
        writer: SourceStreamChunkRowWriter<'a>,
    ) -> ConnectorResult<()> {
        only_parse_payload!(self, payload, writer)
    }
}

mod tests {

    use parquet::arrow;
    use parquet::file::reader::{FileReader, SerializedFileReader};

    #[tokio::test]
    async fn test_parquet_parser() {
        // let file_path = "/path/to/your/parquet/file.parquet";

        // let file = std::fs::File::open(file_path).unwrap();
        // let reader = SerializedFileReader::new(file).unwrap();

        // let file_metadata = reader.metadata();

        // let row_groups = file_metadata.row_groups();

        // println!("File metadata:");
        // println!("  Version: {}", file_metadata.version());
        // println!("  Created by: {}", file_metadata.created_by());
        // println!("  Number of row groups: {}", file_metadata.num_row_groups());
        // println!("  Number of columns: {}", file_metadata.num_columns());
        // println!("  Schema:");

        // for i in 0..file_metadata.num_columns() {
        //     let column_metadata = file_metadata.column(i);
        //     println!("    Column {}: {}", i, column_metadata);
        // }
    }
}
