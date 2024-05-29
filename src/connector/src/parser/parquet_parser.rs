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

    #[allow(clippy::unused_async)]
    pub async fn parse_inner(
        &mut self,
        payload: Vec<u8>,
        mut writer: SourceStreamChunkRowWriter<'_>,
    ) -> ConnectorResult<()> {
        let payload_bytes = bytes::Bytes::from(payload);
        let reader = SerializedFileReader::new(payload_bytes).unwrap();

        let file_metadata = reader.metadata();

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
        let file_path = "/path/to/your/parquet/file.parquet";

        let file = std::fs::File::open(file_path).unwrap();
        let reader = SerializedFileReader::new(file).unwrap();

        let file_metadata = reader.metadata();

        let row_groups = file_metadata.row_groups();

        println!("File metadata:");
        println!("  Version: {}", file_metadata.version());
        println!("  Created by: {}", file_metadata.created_by());
        println!("  Number of row groups: {}", file_metadata.num_row_groups());
        println!("  Number of columns: {}", file_metadata.num_columns());
        println!("  Schema:");

        for i in 0..file_metadata.num_columns() {
            let column_metadata = file_metadata.column(i);
            println!("    Column {}: {}", i, column_metadata);
        }
    }
}
