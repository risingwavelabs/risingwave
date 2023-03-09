// Copyright 2023 RisingWave Labs
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

use std::str::FromStr;

use anyhow::anyhow;
use futures_async_stream::try_stream;
use itertools::Itertools;
use risingwave_common::error::ErrorCode::{InternalError, ProtocolError};
use risingwave_common::error::{Result, RwError};
use risingwave_common::types::{Datum, Decimal, ScalarImpl};
use risingwave_expr::vector_op::cast::{
    str_to_date, str_to_timestamp, str_with_time_zone_to_timestamptz,
};
use simd_json::{BorrowedValue, ValueAccess};

use crate::common::UpsertMessage;
use crate::impl_common_parser_logic;
use crate::parser::common::simd_json_parse_value;
use crate::parser::util::at_least_one_ok;
use crate::parser::{SourceStreamChunkRowWriter, WriteGuard};
use crate::source::{DataType, SourceColumnDesc, SourceContextRef};
impl_common_parser_logic!(CsvParser);
macro_rules! to_rust_type {
    ($v:ident, $t:ty) => {
        $v.parse::<$t>()
            .map_err(|_| anyhow!("failed parse {} from {}", stringify!($t), $v))?
    };
}
#[derive(Debug, Clone)]
pub struct CsvParserConfig {
    pub delimiter: u8,
    pub has_header: bool,
}

/// Parser for JSON format
#[derive(Debug)]
pub struct CsvParser {
    rw_columns: Vec<SourceColumnDesc>,
    source_ctx: SourceContextRef,
    headers: Option<Vec<String>>,
    delimiter: u8,
}

impl CsvParser {
    pub fn new(
        rw_columns: Vec<SourceColumnDesc>,
        parser_config: CsvParserConfig,
        source_ctx: SourceContextRef,
    ) -> Result<Self> {
        let CsvParserConfig {
            delimiter,
            has_header,
        } = parser_config;

        Ok(Self {
            rw_columns,
            delimiter,
            headers: if has_header { Some(Vec::new()) } else { None },
            source_ctx,
        })
    }

    fn read_row(&self, buf: &[u8]) -> Result<Vec<String>> {
        let mut reader_builder = csv::ReaderBuilder::default();
        reader_builder.delimiter(self.delimiter).has_headers(false);
        let record = reader_builder
            .from_reader(buf)
            .records()
            .next()
            .transpose()
            .map_err(|err| RwError::from(ProtocolError(err.to_string())))?;
        Ok(record
            .map(|record| record.iter().map(|field| field.to_string()).collect())
            .unwrap_or_default())
    }

    #[inline]
    fn parse_string(dtype: &DataType, v: String) -> Result<Datum> {
        let v = match dtype {
            // mysql use tinyint to represent boolean
            DataType::Boolean => ScalarImpl::Bool(to_rust_type!(v, i16) != 0),
            DataType::Int16 => ScalarImpl::Int16(to_rust_type!(v, i16)),
            DataType::Int32 => ScalarImpl::Int32(to_rust_type!(v, i32)),
            DataType::Int64 => ScalarImpl::Int64(to_rust_type!(v, i64)),
            DataType::Float32 => ScalarImpl::Float32(to_rust_type!(v, f32).into()),
            DataType::Float64 => ScalarImpl::Float64(to_rust_type!(v, f64).into()),
            // FIXME: decimal should have more precision than f64
            DataType::Decimal => Decimal::from_str(v.as_str())
                .map_err(|_| anyhow!("parse decimal from string err {}", v))?
                .into(),
            DataType::Varchar => v.into(),
            DataType::Date => str_to_date(v.as_str())?.into(),
            DataType::Time => str_to_date(v.as_str())?.into(),
            DataType::Timestamp => str_to_timestamp(v.as_str())?.into(),
            DataType::Timestamptz => str_with_time_zone_to_timestamptz(v.as_str())?.into(),
            _ => {
                return Err(RwError::from(InternalError(format!(
                    "CSV data source not support type {}",
                    dtype
                ))))
            }
        };
        Ok(Some(v))
    }

    #[allow(clippy::unused_async)]
    pub async fn parse_inner(
        &mut self,
        payload: &[u8],
        mut writer: SourceStreamChunkRowWriter<'_>,
    ) -> Result<WriteGuard> {
        let mut fields = self.read_row(payload)?;
        if let Some(headers) = &mut self.headers {
            if headers.is_empty() {
                *headers = fields;
                tracing::warn!("set headers {:?} {:?}", headers, payload);
                // Here we want a row, but got nothing. So it's an error for the `parse_inner` but
                // has no bad impact on the system.
                return  Err(RwError::from(ProtocolError("This messsage indicates a header, no row will be inserted. However, internal parser state was updated.".to_string())));
            }
            writer.insert(|desc| {
                if let Some(i) = headers.iter().position(|name| name == &desc.name) {
                    Self::parse_string(
                        &desc.data_type,
                        fields.get_mut(i).map(std::mem::take).unwrap_or_default(),
                    )
                } else {
                    Ok(None)
                }
            })
        } else {
            fields.reverse();
            writer.insert(|desc| {
                if let Some(value) = fields.pop() {
                    Self::parse_string(&desc.data_type, value)
                } else {
                    Ok(None)
                }
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_parse_row() {
        let parser = CsvParser::new(
            Vec::new(),
            CsvParserConfig {
                delimiter: b',',
                has_header: true,
            },
            Default::default(),
        )
        .unwrap();
        let row = parser.read_row(b"a,b,c").unwrap();
        println!("{:?}", row);
    }
}
