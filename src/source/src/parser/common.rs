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

use num_traits::FromPrimitive;
use risingwave_common::error::ErrorCode::{self, InternalError};
use risingwave_common::error::{Result, RwError};
use risingwave_common::types::{DataType, Decimal, ScalarImpl, ScalarRef};
use risingwave_expr::vector_op::cast::{str_to_date, str_to_timestamp};
use serde_json::Value;

use crate::SourceColumnDesc;

macro_rules! make_ScalarImpl {
    ($x:expr, $y:expr) => {
        match $x {
            Some(v) => return Ok($y(v)),
            None => return Err(RwError::from(InternalError("json parse error".to_string()))),
        }
    };
}

pub(crate) fn json_parse_value(
    column: &SourceColumnDesc,
    value: Option<&Value>,
) -> Result<ScalarImpl> {
    match column.data_type {
        DataType::Boolean => {
            make_ScalarImpl!(value.and_then(|v| v.as_bool()), |x| ScalarImpl::Bool(
                x as bool
            ))
        }
        DataType::Int16 => {
            make_ScalarImpl!(value.and_then(|v| v.as_i64()), |x| ScalarImpl::Int16(
                x as i16
            ))
        }
        DataType::Int32 => {
            make_ScalarImpl!(value.and_then(|v| v.as_i64()), |x| ScalarImpl::Int32(
                x as i32
            ))
        }
        DataType::Int64 => {
            make_ScalarImpl!(value.and_then(|v| v.as_i64()), |x| ScalarImpl::Int64(
                x as i64
            ))
        }
        DataType::Float32 => {
            make_ScalarImpl!(value.and_then(|v| v.as_f64()), |v| ScalarImpl::Float32(
                (v as f32).into()
            ))
        }
        DataType::Float64 => {
            make_ScalarImpl!(
                value.and_then(|v| v.as_f64()),
                |v: f64| ScalarImpl::Float64(v.into())
            )
        }
        DataType::Decimal => match value.and_then(|v| v.as_f64()) {
            Some(v) => match Decimal::from_f64(v) {
                Some(v) => Ok(ScalarImpl::Decimal(v)),
                None => Err(RwError::from(InternalError(
                    "decimal parse error".to_string(),
                ))),
            },
            None => Err(RwError::from(InternalError("json parse error".to_string()))),
        },
        DataType::Varchar => {
            make_ScalarImpl!(value.and_then(|v| v.as_str()), |v: &str| ScalarImpl::Utf8(
                v.to_owned_scalar()
            ))
        }
        DataType::Date => match value.and_then(|v| v.as_str()) {
            None => Err(RwError::from(InternalError("parse error".to_string()))),
            Some(date_str) => Ok(ScalarImpl::NaiveDate(str_to_date(date_str)?)),
        },
        DataType::Timestamp => match value.and_then(|v| v.as_str()) {
            None => Err(RwError::from(InternalError("parse error".to_string()))),
            Some(date_str) => Ok(ScalarImpl::NaiveDateTime(str_to_timestamp(date_str)?)),
        },
        _ => Err(ErrorCode::NotImplemented(
            "unsupported type for json_parse_value".to_string(),
            None.into(),
        )
        .into()),
    }
}
