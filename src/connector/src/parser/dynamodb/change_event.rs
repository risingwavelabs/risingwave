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

use risingwave_common::types::{DataType, DatumCow, ScalarRefImpl, ToDatumRef};

use crate::parser::dynamodb::map_rw_type_to_dynamodb_type;
use crate::parser::unified::{ChangeEvent, ChangeEventOperation};
use crate::parser::{Access, AccessError};
use crate::source::SourceColumnDesc;

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
pub struct DynamodbChangeEvent<A> {
    value_accessor: A,
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
    pub fn new(value_accessor: A) -> Self {
        Self { value_accessor }
    }
}

impl<A> ChangeEvent for DynamodbChangeEvent<A>
where
    A: Access,
{
    fn access_field(&self, desc: &SourceColumnDesc) -> crate::parser::AccessResult<DatumCow<'_>> {
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
