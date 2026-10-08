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

//! Content of this file can be deleted once we stop supporting `create source` syntax v1.
//! New features shall NOT touch this file.

use std::fmt;

use winnow::ModalResult;

use crate::ast::FormatEncodeOptions;
use crate::parser::{Parser, StrError};
use crate::parser_err;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum CompatibleFormatEncode {
    V2(FormatEncodeOptions),
}

impl fmt::Display for CompatibleFormatEncode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CompatibleFormatEncode::V2(inner) => {
                write!(f, "{}", inner)
            }
        }
    }
}

impl CompatibleFormatEncode {
    pub(crate) fn into_v2(self) -> FormatEncodeOptions {
        match self {
            CompatibleFormatEncode::V2(inner) => inner,
        }
    }
}

impl From<FormatEncodeOptions> for CompatibleFormatEncode {
    fn from(value: FormatEncodeOptions) -> Self {
        Self::V2(value)
    }
}

pub fn parse_format_encode(p: &mut Parser<'_>) -> ModalResult<CompatibleFormatEncode> {
    if let Some(schema_v2) = p.parse_schema()? {
        if schema_v2.key_encode.is_some() {
            parser_err!("key encode clause is not supported in source schema");
        }
        Ok(CompatibleFormatEncode::V2(schema_v2))
    } else {
        p.expected("description of the format")
    }
}

pub fn get_delimiter(chars: &str) -> Result<u8, StrError> {
    match chars {
        "," => Ok(b','),   // comma
        ";" => Ok(b';'),   // semicolon
        "\t" => Ok(b'\t'), // tab
        other => Err(StrError(format!(
            "The delimiter should be one of ',', ';', E'\\t', but got {other:?}",
        ))),
    }
}
