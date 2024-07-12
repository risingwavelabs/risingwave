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
use risingwave_common::types::{DataType, DatumCow, ScalarRefImpl, ToDatumRef};

use crate::error::ConnectorResult;
use crate::only_parse_payload;
use crate::parser::dynamodb::{build_dynamodb_json_accessor_builder, map_rw_type_to_dynamodb_type};
use crate::parser::unified::util::apply_row_operation_on_stream_chunk_writer;
use crate::parser::unified::{ChangeEvent, ChangeEventOperation};
use crate::parser::{
    Access, AccessBuilderImpl, AccessError, ByteStreamSourceParser, ParserFormat,
    SourceStreamChunkRowWriter, SpecificParserConfig,
};
use crate::source::{SourceColumnDesc, SourceContext, SourceContextRef};

// See https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_streams_Record.html
// An example of a DynamoDB change event from Kinesis:
// {
//     "awsRegion":"us-east-1",
//     "eventID":"26b1b865-1ee7-47f9-816e-d51cdeef9dc5",
//     "eventName":"MODIFY",
//     "userIdentity":null,
//     "recordFormat":"application/json",
//     "tableName":"wkx-test-orders",
//     "dynamodb":{
//         "ApproximateCreationDateTime":1720046486486729,
//         "Keys":{
//             "customer_name":{
//                 "S":"Bob Lee"
//             },
//             "order_id":{
//                 "N":"3"
//             }
//         },
//         "NewImage":{
//             "order_status":{
//                 "N":"3"
//             },
//             "order_date":{
//                 "N":"1720046486"
//             },
//             "order_id":{
//                 "N":"3"
//             },
//             "price":{
//                 "N":"63.06"
//             },
//             "product_id":{
//                 "N":"2060"
//             },
//             "customer_name":{
//                 "S":"Bob Lee"
//             }
//         },
//         "OldImage":{
//             "order_status":{
//                 "N":"3"
//             },
//             "order_date":{
//                 "N":"1720037677"
//             },
//             "order_id":{
//                 "N":"3"
//             },
//             "price":{
//                 "N":"63.06"
//             },
//             "product_id":{
//                 "N":"2060"
//             },
//             "customer_name":{
//                 "S":"Bob Lee"
//             }
//         },
//         "SizeBytes":192,
//         "ApproximateCreationDateTimePrecision":"MICROSECOND"
//     },
//     "eventSource":"aws:dynamodb"
// }
#[derive(Debug)]
pub struct DynamodbCdcJsonParser {
    payload_builder: AccessBuilderImpl,
    pub(crate) rw_columns: Vec<SourceColumnDesc>,
    source_ctx: SourceContextRef,
    single_jsonb_column: String,
}

impl DynamodbCdcJsonParser {
    pub async fn new(
        props: SpecificParserConfig,
        rw_columns: Vec<SourceColumnDesc>,
        source_ctx: SourceContextRef,
    ) -> ConnectorResult<Self> {
        // the key of Dynamodb CDC are embedded value of primary key and partition key, which is not used here.
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
        let row_op = DynamodbChangeEvent::new(payload_accessor, self.single_jsonb_column.clone());
        match apply_row_operation_on_stream_chunk_writer(&row_op, &mut writer) {
            Ok(_) => Ok(()),
            Err(err) => Err(err)?,
        }
    }
}

impl ByteStreamSourceParser for DynamodbCdcJsonParser {
    fn columns(&self) -> &[SourceColumnDesc] {
        &self.rw_columns
    }

    fn source_ctx(&self) -> &SourceContext {
        &self.source_ctx
    }

    fn parser_format(&self) -> ParserFormat {
        ParserFormat::DynamodbCdcJson
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

struct DynamodbChangeEvent<A> {
    value_accessor: A,
    single_jsonb_column: String,
}

const OLD_IMAGE: &str = "OldImage";
const NEW_IMAGE: &str = "NewImage";
const DYNAMODB: &str = "dynamodb";

const DYNAMODB_CREATE_OP: &str = "INSERT";
const DYNAMODB_MODIFY_OP: &str = "MODIFY";
const DYNAMODB_REMOVE_OP: &str = "REMOVE";

const OP: &str = "eventName";

impl<A> DynamodbChangeEvent<A>
where
    A: Access,
{
    pub fn new(value_accessor: A, single_jsonb_column: String) -> Self {
        Self {
            value_accessor,
            single_jsonb_column,
        }
    }
}

impl<A> ChangeEvent for DynamodbChangeEvent<A>
where
    A: Access,
{
    fn access_field(&self, desc: &SourceColumnDesc) -> crate::parser::AccessResult<DatumCow<'_>> {
        if desc.name == self.single_jsonb_column {
            assert_matches!(desc.data_type, DataType::Jsonb);
            match self.op()? {
                ChangeEventOperation::Delete => self
                    .value_accessor
                    .access(&[DYNAMODB, OLD_IMAGE], &desc.data_type),
                ChangeEventOperation::Upsert => self
                    .value_accessor
                    .access(&[DYNAMODB, NEW_IMAGE], &desc.data_type),
            }
        } else {
            let dynamodb_type = map_rw_type_to_dynamodb_type(&desc.data_type)?;
            match self.op()? {
                ChangeEventOperation::Delete => self.value_accessor.access(
                    &[DYNAMODB, OLD_IMAGE, &desc.name, dynamodb_type.as_str()],
                    &desc.data_type,
                ),
                ChangeEventOperation::Upsert => self.value_accessor.access(
                    &[DYNAMODB, NEW_IMAGE, &desc.name, dynamodb_type.as_str()],
                    &desc.data_type,
                ),
            }
        }
    }

    fn op(&self) -> Result<ChangeEventOperation, AccessError> {
        if let Some(ScalarRefImpl::Utf8(op)) = self
            .value_accessor
            .access(&[OP], &DataType::Varchar)?
            .to_datum_ref()
        {
            match op {
                DYNAMODB_CREATE_OP | DYNAMODB_MODIFY_OP => return Ok(ChangeEventOperation::Upsert),
                DYNAMODB_REMOVE_OP => return Ok(ChangeEventOperation::Delete),
                _ => panic!("Unknown dynamodb event operation: {}", op),
            }
        }
        Err(super::AccessError::Undefined {
            name: "op".into(),
            path: String::from(OP),
        })
    }
}
