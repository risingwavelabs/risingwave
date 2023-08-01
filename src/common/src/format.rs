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

use thiserror::Error;

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

    fn try_from(c: char) -> Result<Self, Self::Error> {
        match c {
            's' => Ok(SpecifierType::SimpleString),
            'I' => Ok(SpecifierType::SqlIdentifier),
            'L' => Ok(SpecifierType::SqlLiteral),
            _ => Err(()),
        }
    }
}

#[derive(Debug)]
pub struct Specifier {
    // TODO: support position, flags and width.
    pub ty: SpecifierType,
}

#[derive(Debug)]
pub enum FormatterNode {
    Specifier(Specifier),
    Literal(String),
}

#[derive(Debug)]
pub struct Formatter {
    nodes: Vec<FormatterNode>,
}

#[derive(Debug, Error)]
pub enum ParseFormatError {
    #[error("unrecognized format() type specifier \"{0}\"")]
    UnrecognizedSpecifierType(char),
    #[error("unterminated format() type specifier")]
    UnterminatedSpecifier,
}

impl Formatter {
    /// Parse the format string into a high-efficient representation.
    /// <https://www.postgresql.org/docs/current/functions-string.html#FUNCTIONS-STRING-FORMAT>
    pub fn parse(format: &str) -> Result<Self, ParseFormatError> {
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

    pub fn nodes(&self) -> &[FormatterNode] {
        &self.nodes
    }
}
