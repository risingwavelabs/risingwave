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

use std::fmt::Write;

use risingwave_common::catalog::Schema;
use risingwave_common::row::Row;
use risingwave_common::types::ToText;

use super::{Result, RowEncoder, SerTo};

/// The XML element name used to wrap each row.
const DEFAULT_ROW_ELEMENT: &str = "row";

/// Encodes a row as an XML element.
///
/// Output format:
/// ```xml
/// <row><col_name>value</col_name><col_name2>value2</col_name2></row>
/// ```
///
/// NULL values are omitted (the element is not emitted).
/// Special characters (`<`, `>`, `&`, `"`, `'`) are escaped.
pub struct XmlEncoder {
    schema: Schema,
    col_indices: Option<Vec<usize>>,
    row_element: String,
}

impl XmlEncoder {
    pub fn new(schema: Schema, col_indices: Option<Vec<usize>>) -> Self {
        Self {
            schema,
            col_indices,
            row_element: DEFAULT_ROW_ELEMENT.to_owned(),
        }
    }
}

/// A wrapper around an XML-encoded `String` that implements `SerTo`.
pub struct XmlLine(pub String);

impl SerTo<String> for XmlLine {
    fn ser_to(self) -> Result<String> {
        Ok(self.0)
    }
}

/// Escape XML special characters.
fn xml_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for ch in s.chars() {
        match ch {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&apos;"),
            _ => out.push(ch),
        }
    }
    out
}

impl RowEncoder for XmlEncoder {
    type Output = XmlLine;

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
        write!(buf, "<{}>", self.row_element).unwrap();
        for idx in col_indices {
            let field = &self.schema[idx];
            match row.datum_at(idx) {
                None => {
                    // Omit NULL fields
                }
                Some(scalar) => {
                    let text = scalar.to_text();
                    let escaped = xml_escape(&text);
                    write!(buf, "<{0}>{1}</{0}>", field.name, escaped).unwrap();
                }
            }
        }
        write!(buf, "</{}>", self.row_element).unwrap();
        Ok(XmlLine(buf))
    }
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
    fn test_xml_encode_basic() {
        let schema = test_schema();
        let encoder = XmlEncoder::new(schema, None);
        let row = OwnedRow::new(vec![
            Some(ScalarImpl::Int32(1)),
            Some(ScalarImpl::Utf8("Alice".into())),
            Some(ScalarImpl::Float64(99.5.into())),
        ]);
        let result: String = encoder.encode(row).unwrap().ser_to().unwrap();
        assert_eq!(
            result,
            "<row><id>1</id><name>Alice</name><score>99.5</score></row>"
        );
    }

    #[test]
    fn test_xml_encode_with_null() {
        let schema = test_schema();
        let encoder = XmlEncoder::new(schema, None);
        let row = OwnedRow::new(vec![
            Some(ScalarImpl::Int32(2)),
            None,
            Some(ScalarImpl::Float64(88.0.into())),
        ]);
        let result: String = encoder.encode(row).unwrap().ser_to().unwrap();
        assert_eq!(result, "<row><id>2</id><score>88</score></row>");
    }

    #[test]
    fn test_xml_encode_escaping() {
        let schema = test_schema();
        let encoder = XmlEncoder::new(schema, None);
        let row = OwnedRow::new(vec![
            Some(ScalarImpl::Int32(3)),
            Some(ScalarImpl::Utf8("<b>Bob & \"friends\"</b>".into())),
            Some(ScalarImpl::Float64(77.0.into())),
        ]);
        let result: String = encoder.encode(row).unwrap().ser_to().unwrap();
        assert_eq!(
            result,
            "<row><id>3</id><name>&lt;b&gt;Bob &amp; &quot;friends&quot;&lt;/b&gt;</name><score>77</score></row>"
        );
    }

    #[test]
    fn test_xml_encode_col_indices() {
        let schema = test_schema();
        let encoder = XmlEncoder::new(schema, Some(vec![0, 2]));
        let row = OwnedRow::new(vec![
            Some(ScalarImpl::Int32(4)),
            Some(ScalarImpl::Utf8("Charlie".into())),
            Some(ScalarImpl::Float64(66.0.into())),
        ]);
        let result: String = encoder.encode(row).unwrap().ser_to().unwrap();
        assert_eq!(result, "<row><id>4</id><score>66</score></row>");
    }

    #[test]
    fn test_xml_encode_all_null() {
        let schema = test_schema();
        let encoder = XmlEncoder::new(schema, None);
        let row = OwnedRow::new(vec![None, None, None]);
        let result: String = encoder.encode(row).unwrap().ser_to().unwrap();
        assert_eq!(result, "<row></row>");
    }
}
