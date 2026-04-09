// Copyright 2026 RisingWave Labs
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

use risingwave_common::catalog::Schema;
use risingwave_common::row::Row;
use risingwave_common::types::ToText;

use super::{Result, RowEncoder, SerTo};

/// Encodes a row as a CSV line (RFC 4180 style).
pub struct CsvEncoder {
    schema: Schema,
    col_indices: Option<Vec<usize>>,
}

impl CsvEncoder {
    pub fn new(schema: Schema, col_indices: Option<Vec<usize>>) -> Self {
        Self {
            schema,
            col_indices,
        }
    }
}

/// A wrapper around a CSV-encoded `String` that implements `SerTo`.
pub struct CsvLine(pub String);

impl SerTo<String> for CsvLine {
    fn ser_to(self) -> Result<String> {
        Ok(self.0)
    }
}

impl RowEncoder for CsvEncoder {
    type Output = CsvLine;

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn col_indices(&self) -> Option<&[usize]> {
        self.col_indices.as_deref()
    }

    fn encode_cols(
        &self,
        row: impl Row,
        col_indices: impl Iterator<Item = usize>,
    ) -> Result<Self::Output> {
        let mut buf = String::new();
        let mut first = true;
        for idx in col_indices {
            if !first {
                buf.push(',');
            }
            first = false;
            match row.datum_at(idx) {
                None => {
                    // NULL is represented as empty (unquoted) field in CSV
                }
                Some(scalar) => {
                    let text = scalar.to_text();
                    // Quote the field if it contains comma, double-quote, or newline
                    if text.contains(',')
                        || text.contains('"')
                        || text.contains('\n')
                        || text.contains('\r')
                    {
                        buf.push('"');
                        for ch in text.chars() {
                            if ch == '"' {
                                buf.push('"'); // escape double-quote by doubling
                            }
                            buf.push(ch);
                        }
                        buf.push('"');
                    } else {
                        buf.push_str(&text);
                    }
                }
            }
        }
        Ok(CsvLine(buf))
    }
}

/// Returns a CSV header line for the given schema and column indices.
pub fn csv_header_line(schema: &Schema, col_indices: Option<&[usize]>) -> String {
    let mut buf = String::new();
    let indices: Box<dyn Iterator<Item = usize>> = match col_indices {
        Some(indices) => Box::new(indices.iter().copied()),
        None => Box::new(0..schema.len()),
    };
    let mut first = true;
    for idx in indices {
        if !first {
            buf.push(',');
        }
        first = false;
        let name = &schema[idx].name;
        if name.contains(',') || name.contains('"') || name.contains('\n') {
            buf.push('"');
            for ch in name.chars() {
                if ch == '"' {
                    buf.push('"');
                }
                buf.push(ch);
            }
            buf.push('"');
        } else {
            buf.push_str(name);
        }
    }
    buf
}

#[cfg(test)]
mod tests {
    use risingwave_common::catalog::Field;
    use risingwave_common::row::OwnedRow;
    use risingwave_common::types::{DataType, ScalarImpl};

    use super::*;

    fn test_schema() -> Schema {
        Schema::new(vec![
            Field::with_name(DataType::Int32, "id"),
            Field::with_name(DataType::Varchar, "name"),
            Field::with_name(DataType::Float64, "score"),
        ])
    }

    #[test]
    fn test_csv_encode_basic() {
        let schema = test_schema();
        let encoder = CsvEncoder::new(schema, None);
        let row = OwnedRow::new(vec![
            Some(ScalarImpl::Int32(1)),
            Some(ScalarImpl::Utf8("Alice".into())),
            Some(ScalarImpl::Float64(99.5.into())),
        ]);
        let result: String = encoder.encode(row).unwrap().ser_to().unwrap();
        assert_eq!(result, "1,Alice,99.5");
    }

    #[test]
    fn test_csv_encode_with_null() {
        let schema = test_schema();
        let encoder = CsvEncoder::new(schema, None);
        let row = OwnedRow::new(vec![
            Some(ScalarImpl::Int32(2)),
            None,
            Some(ScalarImpl::Float64(88.0.into())),
        ]);
        let result: String = encoder.encode(row).unwrap().ser_to().unwrap();
        assert_eq!(result, "2,,88");
    }

    #[test]
    fn test_csv_encode_quoting() {
        let schema = test_schema();
        let encoder = CsvEncoder::new(schema, None);
        let row = OwnedRow::new(vec![
            Some(ScalarImpl::Int32(3)),
            Some(ScalarImpl::Utf8("hello, \"world\"".into())),
            Some(ScalarImpl::Float64(77.0.into())),
        ]);
        let result: String = encoder.encode(row).unwrap().ser_to().unwrap();
        assert_eq!(result, "3,\"hello, \"\"world\"\"\",77");
    }

    #[test]
    fn test_csv_encode_col_indices() {
        let schema = test_schema();
        let encoder = CsvEncoder::new(schema, Some(vec![0, 2]));
        let row = OwnedRow::new(vec![
            Some(ScalarImpl::Int32(4)),
            Some(ScalarImpl::Utf8("Bob".into())),
            Some(ScalarImpl::Float64(66.0.into())),
        ]);
        let result: String = encoder.encode(row).unwrap().ser_to().unwrap();
        assert_eq!(result, "4,66");
    }

    #[test]
    fn test_csv_header_line() {
        let schema = test_schema();
        assert_eq!(csv_header_line(&schema, None), "id,name,score");
        assert_eq!(csv_header_line(&schema, Some(&[0, 2])), "id,score");
    }

    #[test]
    fn test_csv_encode_newline_in_field() {
        let schema = Schema::new(vec![
            Field::with_name(DataType::Int32, "id"),
            Field::with_name(DataType::Varchar, "text"),
        ]);
        let encoder = CsvEncoder::new(schema, None);
        let row = OwnedRow::new(vec![
            Some(ScalarImpl::Int32(1)),
            Some(ScalarImpl::Utf8("line1\nline2".into())),
        ]);
        let result: String = encoder.encode(row).unwrap().ser_to().unwrap();
        assert_eq!(result, "1,\"line1\nline2\"");
    }
}
