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

use super::{ByteStreamSourceParser, ParserFormat, SourceStreamChunkRowWriter};
use crate::error::ConnectorResult;
use crate::only_parse_payload;
use crate::source::{SourceColumnDesc, SourceContext, SourceContextRef};

/// Parser for JSON format
#[derive(Debug)]
pub struct ParquetParser {
    rw_columns: Vec<SourceColumnDesc>,
    source_ctx: SourceContextRef,
}

impl ParquetParser {
    pub fn new() -> Self {
        todo!()
    }
    #[allow(clippy::unused_async)]
    pub async fn parse_inner(
        &mut self,
        payload: Vec<u8>,
        mut writer: SourceStreamChunkRowWriter<'_>,
    ) -> ConnectorResult<()> {
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

    use parquet::basic::Compression;
    use parquet::file::properties::WriterProperties;
    use icelake::io_v2::FileWriter;
    use parquet::schema::types::Type::PrimitiveType;
    use parquet::schema::types::{Type, TypePtr};

    #[tokio::test]
    async fn test_json_parser() {
        // 创建 Parquet 文件的架构
        let schema = TypePtr::new(
            Type::group_type_builder("schema")
                .with_child_field(
                    "name",
                    Type::primitive_type_builder("name", PrimitiveType::UTF8)
                        .build()
                        .unwrap(),
                )
                .with_child_field(
                    "age",
                    Type::primitive_type_builder("age", PrimitiveType::INT32)
                        .build()
                        .unwrap(),
                )
                .build()
                .unwrap(),
        );

        // 创建 Parquet 文件的写入器
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();
        let mut writer = FileWriter::new("example.parquet", schema, props).unwrap();

        // 添加数据行到 Parquet 文件
        let row_group = writer.next_row_group().unwrap();
        let mut name_column = row_group
            .get_column_writer::<parquet::basic::ByteArrayType>(0)
            .unwrap();
        let mut age_column = row_group.get_column_writer::<Int32>(1).unwrap();

        // 添加示例数据
        name_column
            .write_batch(&[
                Some("Alice".as_bytes().to_vec()),
                Some("Bob".as_bytes().to_vec()),
            ])
            .unwrap();
        age_column.write_batch(&[Some(25), Some(30)]).unwrap();

        // 写入文件并关闭写入器
        writer.close().unwrap();
    }
}
