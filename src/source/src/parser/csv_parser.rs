// Copyright 2023 Singularity Data
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
use futures::future::ready;
use risingwave_common::error::ErrorCode::InternalError;
use risingwave_common::error::{Result, RwError};
use risingwave_common::types::{DataType, Datum, Decimal, ScalarImpl};
use risingwave_expr::vector_op::cast::{str_to_date, str_to_timestamp, str_to_timestamptz};

use crate::{ByteStreamSourceParser, ParseFuture, SourceStreamChunkRowWriter, WriteGuard};

macro_rules! to_rust_type {
    ($v:ident, $t:ty) => {
        $v.parse::<$t>()
            .map_err(|_| anyhow!("failed parse {} from {}", stringify!($t), $v))?
    };
}

#[derive(Debug)]
pub struct CsvParser {
    next_row_is_header: bool,
    csv_reader: csv_core::Reader,
    // buffers for parse
    output: Vec<u8>,
    output_cursor: usize,
    ends: Vec<usize>,
    ends_cursor: usize,
}

impl CsvParser {
    pub fn new(delimiter: u8, has_header: bool) -> Result<Self> {
        Ok(Self {
            next_row_is_header: has_header,
            csv_reader: csv_core::ReaderBuilder::new().delimiter(delimiter).build(),
            output: vec![0],
            output_cursor: 0,
            ends: vec![0],
            ends_cursor: 1,
        })
    }

    fn reset_cursor(&mut self) {
        self.output_cursor = 0;
        self.ends_cursor = 1;
    }

    pub fn parse_columns_to_strings(&mut self, chunk: &mut &[u8]) -> Result<Option<Vec<String>>> {
        loop {
            let (result, n_input, n_output, n_ends) = self.csv_reader.read_record(
                chunk,
                &mut self.output[self.output_cursor..],
                &mut self.ends[self.ends_cursor..],
            );
            self.output_cursor += n_output;
            *chunk = &(*chunk)[n_input..];
            self.ends_cursor += n_ends;
            match result {
                // input empty, here means the `chunk` passed to this method
                // doesn't contain a whole record, need more bytes
                csv_core::ReadRecordResult::InputEmpty => break Ok(None),
                // the output buffer is not enough
                csv_core::ReadRecordResult::OutputFull => {
                    let length = self.output.len();
                    self.output.resize(length * 2, 0);
                }
                // the ends buffer is not enough
                csv_core::ReadRecordResult::OutputEndsFull => {
                    let length = self.ends.len();
                    self.ends.resize(length * 2, 0);
                }
                // Success cases
                csv_core::ReadRecordResult::Record | csv_core::ReadRecordResult::End => {
                    // skip the header
                    if self.next_row_is_header {
                        self.next_row_is_header = false;
                        self.reset_cursor();
                        continue;
                    }
                    let ends_cursor = self.ends_cursor;
                    // caller provides an empty chunk, and there is no data
                    // in inner buffer
                    if ends_cursor <= 1 {
                        break Ok(None);
                    }
                    self.reset_cursor();

                    let string_columns = (1..ends_cursor)
                        .map(|culomn| {
                            String::from_utf8(
                                self.output[self.ends[culomn - 1]..self.ends[culomn]].to_owned(),
                            )
                            .map_err(|e| {
                                RwError::from(InternalError(format!(
                                    "Parse csv column {} error: invalid UTF-8 ({})",
                                    culomn, e,
                                )))
                            })
                        })
                        .collect::<Result<Vec<String>>>()?;
                    break Ok(Some(string_columns));
                }
            }
        }
    }

    fn parse_inner(
        &mut self,
        payload: &mut &[u8],
        mut writer: SourceStreamChunkRowWriter<'_>,
    ) -> Result<Option<WriteGuard>> {
        let columns_string = match self.parse_columns_to_strings(payload)? {
            None => return Ok(None),
            Some(strings) => strings,
        };
        writer
            .insert(move |desc| {
                let column_id = desc.column_id.get_id();
                let column_type = &desc.data_type;
                let v = match columns_string.get(column_id as usize) {
                    Some(v) => v.to_owned(),
                    None => return Ok(None),
                };
                parse_string(column_type, v)
            })
            .map(Some)
    }
}

impl ByteStreamSourceParser for CsvParser {
    type ParseResult<'a> = impl ParseFuture<'a, Result<Option<WriteGuard>>>;

    fn parse<'a, 'b, 'c>(
        &'a mut self,
        payload: &'a mut &'b [u8],
        writer: crate::SourceStreamChunkRowWriter<'c>,
    ) -> Self::ParseResult<'a>
    where
        'b: 'a,
        'c: 'a,
    {
        ready(self.parse_inner(payload, writer))
    }
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
        DataType::Timestamptz => str_to_timestamptz(v.as_str())?.into(),
        _ => {
            return Err(RwError::from(InternalError(format!(
                "CSV data source not support type {}",
                dtype
            ))))
        }
    };
    Ok(Some(v))
}

#[cfg(test)]
mod tests {

    use super::*;
    #[tokio::test]
    async fn test_csv_parser_without_last_line_break() {
        let mut parser = CsvParser::new(b',', true).unwrap();
        let data = b"
name,age
pite,20
alex,10";
        let mut part1 = &data[0..data.len() - 1];
        let mut part2 = &data[data.len() - 1..data.len()];
        let line1 = parser.parse_columns_to_strings(&mut part1).unwrap();
        assert!(line1.is_some());
        println!("{:?}", line1);
        let line2 = parser.parse_columns_to_strings(&mut part1).unwrap();
        assert!(line2.is_none());
        let line2 = parser.parse_columns_to_strings(&mut part2).unwrap();
        assert!(line2.is_none());
        let line2 = parser.parse_columns_to_strings(&mut part2).unwrap();
        assert!(line2.is_some());
        println!("{:?}", line2);
    }

    #[tokio::test]
    async fn test_csv_parser_with_last_line_break() {
        let mut parser = CsvParser::new(b',', true).unwrap();
        let data = b"
name,age
pite,20
alex,10
";
        let mut part1 = &data[0..data.len() - 1];
        let mut part2 = &data[data.len() - 1..data.len()];
        let line1 = parser.parse_columns_to_strings(&mut part1).unwrap();
        assert!(line1.is_some());
        println!("{:?}", line1);
        let line2 = parser.parse_columns_to_strings(&mut part1).unwrap();
        assert!(line2.is_none());
        let line2 = parser.parse_columns_to_strings(&mut part2).unwrap();
        assert!(line2.is_some());
        println!("{:?}", line2);
    }
}
