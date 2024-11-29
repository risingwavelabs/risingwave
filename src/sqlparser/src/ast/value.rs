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

#[cfg(not(feature = "std"))]
use alloc::string::String;
use core::fmt;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

use super::ObjectName;

/// Primitive SQL values such as number and string
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum Value {
    /// Numeric literal
    Number(String),
    /// 'string value'
    SingleQuotedString(String),
    // $<tag_name>$string value$<tag_name>$ (postgres syntax)
    DollarQuotedString(DollarQuotedString),
    /// String Constants With C-Style Escapes
    CstyleEscapedString(CstyleEscapedString),
    /// N'string value'
    NationalStringLiteral(String),
    /// X'hex value'
    HexStringLiteral(String),
    DoubleQuotedString(String),
    /// Boolean value true or false
    Boolean(bool),
    /// INTERVAL literals, roughly in the following format:
    /// `INTERVAL '<value>' [ <leading_field> [ (<leading_precision>) ] ]
    /// [ TO <last_field> [ (<fractional_seconds_precision>) ] ]`,
    /// e.g. `INTERVAL '123:45.67' MINUTE(3) TO SECOND(2)`.
    ///
    /// The parser does not validate the `<value>`, nor does it ensure
    /// that the `<leading_field>` units >= the units in `<last_field>`,
    /// so the user will have to reject intervals like `HOUR TO YEAR`.
    Interval {
        value: String,
        leading_field: Option<DateTimeField>,
        leading_precision: Option<u64>,
        last_field: Option<DateTimeField>,
        /// The seconds precision can be specified in SQL source as
        /// `INTERVAL '__' SECOND(_, x)` (in which case the `leading_field`
        /// will be `Second` and the `last_field` will be `None`),
        /// or as `__ TO SECOND(x)`.
        fractional_seconds_precision: Option<u64>,
    },
    /// `NULL` value
    Null,
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Number(v) => write!(f, "{}", v),
            Value::DoubleQuotedString(v) => write!(f, "\"{}\"", v),
            Value::SingleQuotedString(v) => write!(f, "'{}'", escape_single_quote_string(v)),
            Value::DollarQuotedString(v) => write!(f, "{}", v),
            Value::NationalStringLiteral(v) => write!(f, "N'{}'", v),
            Value::HexStringLiteral(v) => write!(f, "X'{}'", v),
            Value::CstyleEscapedString(v) => write!(f, "E'{}'", v),
            Value::Boolean(v) => write!(f, "{}", v),
            Value::Interval {
                value,
                leading_field: Some(DateTimeField::Second),
                leading_precision: Some(leading_precision),
                last_field,
                fractional_seconds_precision: Some(fractional_seconds_precision),
            } => {
                // When the leading field is SECOND, the parser guarantees that
                // the last field is None.
                assert!(last_field.is_none());
                write!(
                    f,
                    "INTERVAL '{}' SECOND ({}, {})",
                    escape_single_quote_string(value),
                    leading_precision,
                    fractional_seconds_precision
                )
            }
            Value::Interval {
                value,
                leading_field,
                leading_precision,
                last_field,
                fractional_seconds_precision,
            } => {
                write!(f, "INTERVAL '{}'", escape_single_quote_string(value))?;
                if let Some(leading_field) = leading_field {
                    write!(f, " {}", leading_field)?;
                }
                if let Some(leading_precision) = leading_precision {
                    write!(f, " ({})", leading_precision)?;
                }
                if let Some(last_field) = last_field {
                    write!(f, " TO {}", last_field)?;
                }
                if let Some(fractional_seconds_precision) = fractional_seconds_precision {
                    write!(f, " ({})", fractional_seconds_precision)?;
                }
                Ok(())
            }
            Value::Null => write!(f, "NULL"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct DollarQuotedString {
    pub value: String,
    pub tag: Option<String>,
}

impl fmt::Display for DollarQuotedString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.tag {
            Some(tag) => {
                write!(f, "${}${}${}$", tag, self.value, tag)
            }
            None => {
                write!(f, "$${}$$", self.value)
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct CstyleEscapedString {
    /// The unescaped string.
    pub value: String,
    /// The raw string used for simplifying `fmt::Display` (unparsing) implementation.
    pub raw: String,
}

impl fmt::Display for CstyleEscapedString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.raw)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum DateTimeField {
    Year,
    Month,
    Day,
    Hour,
    Minute,
    Second,
}

impl fmt::Display for DateTimeField {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            DateTimeField::Year => "YEAR",
            DateTimeField::Month => "MONTH",
            DateTimeField::Day => "DAY",
            DateTimeField::Hour => "HOUR",
            DateTimeField::Minute => "MINUTE",
            DateTimeField::Second => "SECOND",
        })
    }
}

pub struct EscapeSingleQuoteString<'a>(&'a str);

impl fmt::Display for EscapeSingleQuoteString<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for c in self.0.chars() {
            if c == '\'' {
                write!(f, "\'\'")?;
            } else {
                write!(f, "{}", c)?;
            }
        }
        Ok(())
    }
}

pub fn escape_single_quote_string(s: &str) -> EscapeSingleQuoteString<'_> {
    EscapeSingleQuoteString(s)
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum TrimWhereField {
    Both,
    Leading,
    Trailing,
}

impl fmt::Display for TrimWhereField {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use TrimWhereField::*;
        f.write_str(match self {
            Both => "BOTH",
            Leading => "LEADING",
            Trailing => "TRAILING",
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum JsonPredicateType {
    #[default]
    Value,
    Array,
    Object,
    Scalar,
}

impl fmt::Display for JsonPredicateType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use JsonPredicateType::*;
        f.write_str(match self {
            Value => "",
            Array => " ARRAY",
            Object => " OBJECT",
            Scalar => " SCALAR",
        })
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct SecretRefValue {
    pub secret_name: ObjectName,
    pub ref_as: SecretRefAsType,
}

impl fmt::Display for SecretRefValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.ref_as {
            SecretRefAsType::Text => write!(f, "{}", self.secret_name),
            SecretRefAsType::File => write!(f, "{} AS FILE", self.secret_name),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum SecretRefAsType {
    Text,
    File,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct ConnectionRefValue {
    pub connection_name: ObjectName,
}

impl fmt::Display for ConnectionRefValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.connection_name)
    }
}
