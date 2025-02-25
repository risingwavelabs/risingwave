// Copyright 2025 RisingWave Labs
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
use std::str::FromStr;

use risingwave_common::row::Row;
use risingwave_common::types::{ScalarRefImpl, ToText};
use risingwave_expr::{ExprError, Result, function};

use super::string::quote_ident;

/// Formats arguments according to a format string.
///
/// # Example
///
/// ```slt
/// query T
/// select format('%s %s', 'Hello', 'World');
/// ----
/// Hello World
///
/// query T
/// select format('%s %s', variadic array['Hello', 'World']);
/// ----
/// Hello World
/// ```
#[function(
    "format(varchar, variadic anyarray) -> varchar",
    prebuild = "Formatter::from_str($0).map_err(|e| ExprError::Parse(e.to_report_string().into()))?"
)]
fn format(row: impl Row, formatter: &Formatter, writer: &mut impl Write) -> Result<()> {
    let mut args = row.iter();
    for node in &formatter.nodes {
        match node {
            FormatterNode::Literal(literal) => writer.write_str(literal).unwrap(),
            FormatterNode::Specifier(sp) => {
                let arg = args.next().ok_or(ExprError::TooFewArguments)?;
                match sp.ty {
                    SpecifierType::SimpleString => {
                        if let Some(scalar) = arg {
                            scalar.write(writer).unwrap();
                        }
                    }
                    SpecifierType::SqlIdentifier => match arg {
                        Some(ScalarRefImpl::Utf8(arg)) => quote_ident(arg, writer),
                        _ => {
                            return Err(ExprError::UnsupportedFunction(
                                "unsupported data for specifier type 'I'".to_owned(),
                            ));
                        }
                    },
                    SpecifierType::SqlLiteral => {
                        return Err(ExprError::UnsupportedFunction(
                            "unsupported specifier type 'L'".to_owned(),
                        ));
                    }
                }
            }
        }
    }
    Ok(())
}

/// The type of format conversion to use to produce the format specifier's output.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum SpecifierType {
    /// `s` formats the argument value as a simple string. A null value is treated as an empty
    /// string.
    SimpleString,
    /// `I` treats the argument value as an SQL identifier, double-quoting it if necessary. It is
    /// an error for the value to be null (equivalent to `quote_ident`).
    SqlIdentifier,
    /// `L` quotes the argument value as an SQL literal. A null value is displayed as the string
    /// NULL, without quotes (equivalent to `quote_nullable`).
    SqlLiteral,
}

impl TryFrom<char> for SpecifierType {
    type Error = ();

    fn try_from(c: char) -> std::result::Result<Self, Self::Error> {
        match c {
            's' => Ok(SpecifierType::SimpleString),
            'I' => Ok(SpecifierType::SqlIdentifier),
            'L' => Ok(SpecifierType::SqlLiteral),
            _ => Err(()),
        }
    }
}

#[derive(Debug)]
struct Specifier {
    // TODO: support position, flags and width.
    ty: SpecifierType,
}

#[derive(Debug)]
enum FormatterNode {
    Specifier(Specifier),
    Literal(String),
}

#[derive(Debug)]
struct Formatter {
    nodes: Vec<FormatterNode>,
}

#[derive(Debug, thiserror::Error)]
enum ParseFormatError {
    #[error("unrecognized format() type specifier \"{0}\"")]
    UnrecognizedSpecifierType(char),
    #[error("unterminated format() type specifier")]
    UnterminatedSpecifier,
}

impl FromStr for Formatter {
    type Err = ParseFormatError;

    /// Parse the format string into a high-efficient representation.
    /// <https://www.postgresql.org/docs/current/functions-string.html#FUNCTIONS-STRING-FORMAT>
    fn from_str(format: &str) -> std::result::Result<Self, ParseFormatError> {
        // 8 is a good magic number here, it can cover an input like 'Testing %s, %s, %s, %%'.
        let mut nodes = Vec::with_capacity(8);
        let mut after_percent = false;
        let mut literal = String::with_capacity(8);
        for c in format.chars() {
            if after_percent && c == '%' {
                literal.push('%');
                after_percent = false;
            } else if after_percent {
                // TODO: support position, flags and width.
                if let Ok(ty) = SpecifierType::try_from(c) {
                    if !literal.is_empty() {
                        nodes.push(FormatterNode::Literal(std::mem::take(&mut literal)));
                    }
                    nodes.push(FormatterNode::Specifier(Specifier { ty }));
                } else {
                    return Err(ParseFormatError::UnrecognizedSpecifierType(c));
                }
                after_percent = false;
            } else if c == '%' {
                after_percent = true;
            } else {
                literal.push(c);
            }
        }

        if after_percent {
            return Err(ParseFormatError::UnterminatedSpecifier);
        }

        if !literal.is_empty() {
            nodes.push(FormatterNode::Literal(literal));
        }

        Ok(Formatter { nodes })
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::array::DataChunk;
    use risingwave_common::row::Row;
    use risingwave_common::test_prelude::DataChunkTestExt;
    use risingwave_common::types::ToOwnedDatum;
    use risingwave_common::util::iter_util::ZipEqDebug;
    use risingwave_expr::expr::build_from_pretty;

    #[tokio::test]
    async fn test_format() {
        let format = build_from_pretty("(format:varchar $0:varchar $1:varchar $2:varchar)");
        let (input, expected) = DataChunk::from_pretty(
            "T          T       T       T
             Hello%s    World   .       HelloWorld
             %s%s       Hello   World   HelloWorld
             %I         &&      .       \"&&\"
             .          a       b       .",
        )
        .split_column_at(3);

        // test eval
        let output = format.eval(&input).await.unwrap();
        assert_eq!(&output, expected.column_at(0));

        // test eval_row
        for (row, expected) in input.rows().zip_eq_debug(expected.rows()) {
            let result = format.eval_row(&row.to_owned_row()).await.unwrap();
            assert_eq!(result, expected.datum_at(0).to_owned_datum());
        }
    }
}
