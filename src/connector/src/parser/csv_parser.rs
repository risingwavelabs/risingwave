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
use risingwave_common::cast::{str_to_date, str_to_timestamp};
use risingwave_common::error::ErrorCode::{InternalError, ProtocolError};
use risingwave_common::error::{Result, RwError};
use risingwave_common::types::{Datum, Decimal, ScalarImpl, Timestamptz};

use super::ByteStreamSourceParser;
use crate::parser::{SourceStreamChunkRowWriter, WriteGuard};
use crate::source::{DataType, SourceColumnDesc, SourceContext, SourceContextRef};

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

/// Parser for CSV format
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
            DataType::Timestamptz => ScalarImpl::Timestamptz(to_rust_type!(v, Timestamptz).into()),
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
        payload: Vec<u8>,
        mut writer: SourceStreamChunkRowWriter<'_>,
    ) -> Result<WriteGuard> {
        let mut fields = self.read_row(&payload)?;
        if let Some(headers) = &mut self.headers {
            if headers.is_empty() {
                *headers = fields;
                // Here we want a row, but got nothing. So it's an error for the `parse_inner` but
                // has no bad impact on the system.
                return  Err(RwError::from(ProtocolError("This message indicates a header, no row will be inserted. However, internal parser state was updated.".to_string())));
            }
            writer.insert(|desc| {
                if let Some(i) = headers.iter().position(|name| name == &desc.name) {
                    let value = fields.get_mut(i).map(std::mem::take).unwrap_or_default();
                    if value.is_empty() {
                        return Ok(None);
                    }
                    Self::parse_string(&desc.data_type, value)
                } else {
                    Ok(None)
                }
            })
        } else {
            fields.reverse();
            writer.insert(|desc| {
                if let Some(value) = fields.pop() {
                    if value.is_empty() {
                        return Ok(None);
                    }
                    Self::parse_string(&desc.data_type, value)
                } else {
                    Ok(None)
                }
            })
        }
    }
}

impl ByteStreamSourceParser for CsvParser {
    fn columns(&self) -> &[SourceColumnDesc] {
        &self.rw_columns
    }

    fn source_ctx(&self) -> &SourceContext {
        &self.source_ctx
    }

    async fn parse_one<'a>(
        &'a mut self,
        payload: Vec<u8>,
        writer: SourceStreamChunkRowWriter<'a>,
    ) -> Result<WriteGuard> {
        self.parse_inner(payload, writer).await
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::array::Op;
    use risingwave_common::row::Row;
    use risingwave_common::types::{DataType, ScalarImpl, ToOwnedDatum};

    use super::*;
    use crate::parser::SourceStreamChunkBuilder;
    #[tokio::test]
    async fn test_csv_without_headers() {
        let data = vec![
            r#"1,a,2"#,
            r#""15541","a,1,1,",4"#,
            r#"0,"""0",0"#,
            r#"0,0,0,0,0,0,0,0,0,0,0,0,0,"#,
            r#",,,,"#,
        ];
        let descs = vec![
            SourceColumnDesc::simple("a", DataType::Int32, 0.into()),
            SourceColumnDesc::simple("b", DataType::Varchar, 1.into()),
            SourceColumnDesc::simple("c", DataType::Int32, 2.into()),
        ];
        let mut parser = CsvParser::new(
            Vec::new(),
            CsvParserConfig {
                delimiter: b',',
                has_header: false,
            },
            Default::default(),
        )
        .unwrap();
        let mut builder = SourceStreamChunkBuilder::with_capacity(descs, 4);
        for item in data {
            parser
                .parse_inner(item.as_bytes().to_vec(), builder.row_writer())
                .await
                .unwrap();
        }
        let chunk = builder.finish();
        let mut rows = chunk.rows();
        {
            let (op, row) = rows.next().unwrap();
            assert_eq!(op, Op::Insert);
            assert_eq!(
                row.datum_at(0).to_owned_datum(),
                (Some(ScalarImpl::Int32(1)))
            );
            assert_eq!(
                row.datum_at(1).to_owned_datum(),
                (Some(ScalarImpl::Utf8("a".into())))
            );
            assert_eq!(
                row.datum_at(2).to_owned_datum(),
                (Some(ScalarImpl::Int32(2)))
            );
        }
        {
            let (op, row) = rows.next().unwrap();
            assert_eq!(op, Op::Insert);
            assert_eq!(
                row.datum_at(0).to_owned_datum(),
                (Some(ScalarImpl::Int32(15541)))
            );
            assert_eq!(
                row.datum_at(1).to_owned_datum(),
                (Some(ScalarImpl::Utf8("a,1,1,".into())))
            );
            assert_eq!(
                row.datum_at(2).to_owned_datum(),
                (Some(ScalarImpl::Int32(4)))
            );
        }

        {
            let (op, row) = rows.next().unwrap();
            assert_eq!(op, Op::Insert);
            assert_eq!(
                row.datum_at(0).to_owned_datum(),
                (Some(ScalarImpl::Int32(0)))
            );
            assert_eq!(
                row.datum_at(1).to_owned_datum(),
                (Some(ScalarImpl::Utf8("\"0".into())))
            );
            assert_eq!(
                row.datum_at(2).to_owned_datum(),
                (Some(ScalarImpl::Int32(0)))
            );
        }

        {
            let (op, row) = rows.next().unwrap();
            assert_eq!(op, Op::Insert);
            assert_eq!(
                row.datum_at(0).to_owned_datum(),
                (Some(ScalarImpl::Int32(0)))
            );
            assert_eq!(
                row.datum_at(1).to_owned_datum(),
                (Some(ScalarImpl::Utf8("0".into())))
            );
            assert_eq!(
                row.datum_at(2).to_owned_datum(),
                (Some(ScalarImpl::Int32(0)))
            );
        }

        {
            let (op, row) = rows.next().unwrap();
            assert_eq!(op, Op::Insert);
            assert_eq!(row.datum_at(0), None);
            assert_eq!(row.datum_at(1), None);
            assert_eq!(row.datum_at(2), None);
        }
    }
    #[tokio::test]
    async fn test_csv_with_headers() {
        let data = [
            r#"c,b,a"#,
            r#"1,a,2"#,
            r#""15541","a,1,1,",4"#,
            r#"0,"""0",0"#,
            r#"0,0,0,0,0,0,0,0,0,0,0,0,0,"#,
        ];
        let descs = vec![
            SourceColumnDesc::simple("a", DataType::Int32, 0.into()),
            SourceColumnDesc::simple("b", DataType::Varchar, 1.into()),
            SourceColumnDesc::simple("c", DataType::Int32, 2.into()),
        ];
        let mut parser = CsvParser::new(
            Vec::new(),
            CsvParserConfig {
                delimiter: b',',
                has_header: true,
            },
            Default::default(),
        )
        .unwrap();
        let mut builder = SourceStreamChunkBuilder::with_capacity(descs, 4);
        for item in data {
            let _ = parser
                .parse_inner(item.as_bytes().to_vec(), builder.row_writer())
                .await;
        }
        let chunk = builder.finish();
        let mut rows = chunk.rows();
        {
            let (op, row) = rows.next().unwrap();
            assert_eq!(op, Op::Insert);
            assert_eq!(
                row.datum_at(2).to_owned_datum(),
                (Some(ScalarImpl::Int32(1)))
            );
            assert_eq!(
                row.datum_at(1).to_owned_datum(),
                (Some(ScalarImpl::Utf8("a".into())))
            );
            assert_eq!(
                row.datum_at(0).to_owned_datum(),
                (Some(ScalarImpl::Int32(2)))
            );
        }
        {
            let (op, row) = rows.next().unwrap();
            assert_eq!(op, Op::Insert);
            assert_eq!(
                row.datum_at(2).to_owned_datum(),
                (Some(ScalarImpl::Int32(15541)))
            );
            assert_eq!(
                row.datum_at(1).to_owned_datum(),
                (Some(ScalarImpl::Utf8("a,1,1,".into())))
            );
            assert_eq!(
                row.datum_at(0).to_owned_datum(),
                (Some(ScalarImpl::Int32(4)))
            );
        }

        {
            let (op, row) = rows.next().unwrap();
            assert_eq!(op, Op::Insert);
            assert_eq!(
                row.datum_at(2).to_owned_datum(),
                (Some(ScalarImpl::Int32(0)))
            );
            assert_eq!(
                row.datum_at(1).to_owned_datum(),
                (Some(ScalarImpl::Utf8("\"0".into())))
            );
            assert_eq!(
                row.datum_at(0).to_owned_datum(),
                (Some(ScalarImpl::Int32(0)))
            );
        }

        {
            let (op, row) = rows.next().unwrap();
            assert_eq!(op, Op::Insert);
            assert_eq!(
                row.datum_at(2).to_owned_datum(),
                (Some(ScalarImpl::Int32(0)))
            );
            assert_eq!(
                row.datum_at(1).to_owned_datum(),
                (Some(ScalarImpl::Utf8("0".into())))
            );
            assert_eq!(
                row.datum_at(0).to_owned_datum(),
                (Some(ScalarImpl::Int32(0)))
            );
        }
    }
}
