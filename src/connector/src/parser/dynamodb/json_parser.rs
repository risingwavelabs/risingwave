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

use assert_matches::assert_matches;
use risingwave_common::types::DataType;
use risingwave_connector_codec::decoder::Access;

use crate::error::ConnectorResult;
use crate::only_parse_payload;
use crate::parser::dynamodb::{build_dynamodb_json_accessor_builder, map_rw_type_to_dynamodb_type};
use crate::parser::{
    AccessBuilderImpl, ByteStreamSourceParser, ParserFormat, SourceStreamChunkRowWriter,
    SpecificParserConfig,
};
use crate::source::{SourceColumnDesc, SourceContext, SourceContextRef};

const ITEM: &str = "Item";

// An example of a DynamoDB event on S3:
// {
//     "Item":{
//         "customer_name":{
//             "S":"Bob Lee"
//         },
//         "order_id":{
//             "N":"3"
//         },
//         "price":{
//             "N":"63.06"
//         },
//         "order_status":{
//             "N":"3"
//         },
//         "product_id":{
//             "N":"2060"
//         },
//         "order_date":{
//             "N":"1720037677"
//         }
//     }
// }
#[derive(Debug)]
pub struct DynamodbJsonParser {
    payload_builder: AccessBuilderImpl,
    pub(crate) rw_columns: Vec<SourceColumnDesc>,
    source_ctx: SourceContextRef,
    single_jsonb_column: String,
}

impl DynamodbJsonParser {
    pub async fn new(
        props: SpecificParserConfig,
        rw_columns: Vec<SourceColumnDesc>,
        source_ctx: SourceContextRef,
    ) -> ConnectorResult<Self> {
        let (payload_builder, single_jsonb_column) =
            build_dynamodb_json_accessor_builder(props.encoding_config).await?;
        Ok(Self {
            payload_builder,
            rw_columns,
            source_ctx,
            single_jsonb_column,
        })
    }

    pub async fn parse_inner(
        &mut self,
        payload: Vec<u8>,
        mut writer: SourceStreamChunkRowWriter<'_>,
    ) -> ConnectorResult<()> {
        let payload_accessor = self.payload_builder.generate_accessor(payload).await?;
        writer.do_insert(|column| {
            if column.name == self.single_jsonb_column {
                assert_matches!(column.data_type, DataType::Jsonb);
                payload_accessor.access(&[ITEM], &column.data_type)
            } else {
                let dynamodb_type = map_rw_type_to_dynamodb_type(&column.data_type)?;
                payload_accessor.access(
                    &[ITEM, &column.name, dynamodb_type.as_str()],
                    &column.data_type,
                )
            }
        })?;
        Ok(())
    }
}

impl ByteStreamSourceParser for DynamodbJsonParser {
    fn columns(&self) -> &[SourceColumnDesc] {
        &self.rw_columns
    }

    fn source_ctx(&self) -> &SourceContext {
        &self.source_ctx
    }

    fn parser_format(&self) -> ParserFormat {
        ParserFormat::DynamodbJson
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
