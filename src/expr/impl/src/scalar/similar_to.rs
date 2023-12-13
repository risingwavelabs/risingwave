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

use std::fmt::Write;

use risingwave_expr::{function, ExprError, Result};

fn similar_escape_internal(
    pat: &str,
    esc_text: Option<char>,
    writer: &mut impl Write,
) -> std::result::Result<(), std::fmt::Error> {
    writer.write_str("^(?:")?;

    let mut afterescape = false;
    let mut incharclass = false;

    for chr in pat.chars() {
        match chr {
            c if afterescape => {
                writer.write_char('\\')?;
                writer.write_char(c)?;

                afterescape = false;
            }
            c if esc_text.is_some() && c == esc_text.unwrap() => {
                afterescape = true;
            }
            c if incharclass => {
                if c == '\\' {
                    writer.write_char('\\')?;
                }
                writer.write_char(c)?;

                if c == ']' {
                    incharclass = false;
                }
            }
            c @ '[' => {
                writer.write_char(c)?;
                incharclass = true;
            }
            '%' => {
                writer.write_str(".*")?;
            }
            '_' => {
                writer.write_char('.')?;
            }
            '(' => {
                // convert to non-capturing parenthesis
                writer.write_str("(:?")?;
            }
            c @ ('\\' | '.' | '^' | '$') => {
                writer.write_char('\\')?;
                writer.write_char(c)?;
            }
            c => {
                writer.write_char(c)?;
            }
        }
    }

    writer.write_str(")$")
}

#[function(
    // x SIMILAR TO y -> x ~ similar_to_escape(y)
    "similar_to_escape(varchar) -> varchar",
)]
fn similar_to_escape_default(pat: &str, writer: &mut impl Write) -> Result<()> {
    similar_escape_internal(pat, Some('\\'), writer).map_err(|e| ExprError::Internal(e.into()))
}
#[function("similar_to_escape(varchar, varchar) -> varchar")]
fn similar_to_escape_with_escape_text(
    pat: &str,
    esc_text: &str,
    writer: &mut impl Write,
) -> Result<()> {
    if esc_text.len() > 1 {
        return Err(ExprError::InvalidParam {
            name: "escape string",
            reason: format!(
                "Invalid escape string: `{}`, must be empty or one character",
                esc_text
            )
            .into(),
        });
    }

    similar_escape_internal(pat, esc_text.chars().nth(0), writer)
        .map_err(|e| ExprError::Internal(e.into()))
}
