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

mod json_parser;
pub use json_parser::*;
mod cdc_json_parser;
pub use cdc_json_parser::*;
use risingwave_common::bail;
use risingwave_common::types::DataType;
use risingwave_connector_codec::decoder::AccessError;

use crate::error::ConnectorResult;
use crate::parser::{AccessBuilderImpl, EncodingProperties, JsonAccessBuilder};

pub(crate) async fn build_dynamodb_json_accessor_builder(
    config: EncodingProperties,
) -> ConnectorResult<(AccessBuilderImpl, String)> {
    match config {
        EncodingProperties::Json(json_config) => {
            assert!(json_config.single_blob_column.is_some());
            let single_blob_column = json_config.single_blob_column.clone().unwrap();
            Ok((
                AccessBuilderImpl::Json(JsonAccessBuilder::new_for_dynamodb(json_config)?),
                single_blob_column,
            ))
        }
        _ => bail!("unsupported encoding for Dynamodb"),
    }
}

pub fn map_rw_type_to_dynamodb_type(rw_type: &DataType) -> crate::parser::AccessResult<String> {
    Ok(match rw_type {
        DataType::Boolean => todo!(),
        DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::Float32
        | DataType::Float64
        | DataType::Int256
        | DataType::Decimal => String::from("N"),
        DataType::Date
        | DataType::Varchar
        | DataType::Time
        | DataType::Timestamp
        | DataType::Timestamptz
        | DataType::Interval => String::from("S"),
        DataType::Bytea => String::from("B"),
        // Struct(_), List(_), Jsonb, Serial are not supported yet
        DataType::Struct(_) | DataType::List(_) | DataType::Jsonb | DataType::Serial => {
            return Err(AccessError::UnsupportedType {
                ty: format!("{rw_type:?}"),
            });
        }
    })
}
