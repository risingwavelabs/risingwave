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

use risingwave_common::bail_not_implemented;
use risingwave_common::types::DataType;
use risingwave_sqlparser::ast::{DataType as AstDataType, StructField};

use crate::error::Result;

#[easy_ext::ext(DataTypeToAst)]
impl DataType {
    pub fn to_ast(&self) -> Result<AstDataType> {
        match self {
            DataType::Boolean => Ok(AstDataType::Boolean),
            DataType::Int16 => Ok(AstDataType::SmallInt),
            DataType::Int32 => Ok(AstDataType::Int),
            DataType::Int64 => Ok(AstDataType::BigInt),
            DataType::Float32 => Ok(AstDataType::Real),
            DataType::Float64 => Ok(AstDataType::Double),
            // TODO: handle precision and scale for decimal
            DataType::Decimal => Ok(AstDataType::Decimal(None, None)),
            DataType::Date => Ok(AstDataType::Date),
            DataType::Varchar => Ok(AstDataType::Varchar),
            DataType::Time => Ok(AstDataType::Time(false)),
            DataType::Timestamp => Ok(AstDataType::Timestamp(false)),
            DataType::Timestamptz => Ok(AstDataType::Timestamp(true)),
            DataType::Interval => Ok(AstDataType::Interval),
            DataType::Jsonb => Ok(AstDataType::Jsonb),
            DataType::Bytea => Ok(AstDataType::Bytea),
            DataType::List(item_ty) => Ok(AstDataType::Array(Box::new(item_ty.to_ast()?))),
            DataType::Struct(fields) => {
                let fields = fields
                    .iter()
                    .map(|(name, ty)| {
                        ty.to_ast().map(|ty| StructField {
                            name: name.into(),
                            data_type: ty,
                        })
                    })
                    .try_collect()?;
                Ok(AstDataType::Struct(fields))
            }
            DataType::Serial | DataType::Int256 | DataType::Map(_) => {
                // TODO: support them
                bail_not_implemented!("convert data type {:?} back to AST", self);
            }
        }
    }
}
