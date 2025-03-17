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

use risingwave_expr::{ExprError, Result, function};

// escape `similar-to` pattern to POSIX regex pattern
// Adapted from:
// https://github.com/postgres/postgres/blob/db4f21e4a34b1d5a3f7123e28e77f575d1a971ea/src/backend/utils/adt/regexp.c#L768
fn similar_escape_internal(
    pat: &str,
    esc_text: Option<char>,
    writer: &mut impl Write,
) -> std::result::Result<(), ExprError> {
    macro_rules! write_ {
        ($s:expr) => {
            write!(writer, "{}", $s).unwrap()
        };
    }

    write_!("^(?:");

    let mut nquotes = 0;
    let mut afterescape = false;
    let mut incharclass = false;

    for chr in pat.chars() {
        match chr {
            c if afterescape => {
                if c == '"' && !incharclass {
                    match nquotes {
                        0 => write_!("){1,1}?("),
                        1 => write_!("){1,1}(?:"),
                        _ => {
                            return Err(ExprError::InvalidParam {
                                name: "pat",
                                reason: "SQL regular expression may not contain more than two escape-double-quote separators".into()
                            });
                        }
                    }
                    nquotes += 1;
                } else {
                    write_!('\\');
                    write_!(c);
                }

                afterescape = false;
            }
            c if esc_text.is_some_and(|t| t == c) => {
                afterescape = true;
            }
            c if incharclass => {
                if c == '\\' {
                    write_!('\\');
                }
                write_!(c);

                if c == ']' {
                    incharclass = false;
                }
            }
            c @ '[' => {
                write_!(c);
                incharclass = true;
            }
            '%' => {
                write_!(".*");
            }
            '_' => {
                write_!('.');
            }
            '(' => {
                // convert to non-capturing parenthesis
                write_!("(?:");
            }
            c @ ('\\' | '.' | '^' | '$') => {
                write_!('\\');
                write_!(c);
            }
            c => {
                write_!(c);
            }
        }
    }

    write_!(")$");

    Ok(())
}

#[function(
    // x SIMILAR TO y -> x ~ similar_to_escape(y)
    "similar_to_escape(varchar) -> varchar",
)]
fn similar_to_escape_default(pat: &str, writer: &mut impl Write) -> Result<()> {
    similar_escape_internal(pat, Some('\\'), writer)
}

#[function(
    // x SIMILAR TO y ESCAPE z -> x ~ similar_to_escape(y, z)
    "similar_to_escape(varchar, varchar) -> varchar"
)]
fn similar_to_escape_with_escape_text(
    pat: &str,
    esc_text: &str,
    writer: &mut impl Write,
) -> Result<()> {
    if esc_text.chars().nth(1).is_some() {
        return Err(ExprError::InvalidParam {
            name: "escape string",
            reason: format!(
                "Invalid escape string: `{}`, must be empty or one character",
                esc_text
            )
            .into(),
        });
    }

    similar_escape_internal(pat, esc_text.chars().next(), writer)
}

#[cfg(test)]
mod tests {
    use super::{similar_to_escape_default, similar_to_escape_with_escape_text};

    #[test]
    fn test_default_escape() {
        let cases = vec![
            ("", "^(?:)$"),
            ("_bcd%", r#"^(?:.bcd.*)$"#),
            ("bcd%", r#"^(?:bcd.*)$"#),
            (r#"_bcd\%"#, r#"^(?:.bcd\%)$"#),
            ("bcd[]ee", "^(?:bcd[]ee)$"),
            (r#"bcd[]"ee""#, r#"^(?:bcd[]"ee")$"#),
            ("bcd[pp_%.]ee", "^(?:bcd[pp_%.]ee)$"),
            ("bcd[pp_%.]ee_%.", r#"^(?:bcd[pp_%.]ee..*\.)$"#),
            ("bcd[pp_%.](ee_%.)", r#"^(?:bcd[pp_%.](?:ee..*\.))$"#),
            (r#"%\"o_b\"%"#, "^(?:.*){1,1}?(o.b){1,1}(?:.*)$"),
        ];

        for (pat, escaped) in cases {
            let mut writer = String::new();
            similar_to_escape_default(pat, &mut writer).ok();
            assert_eq!(writer, escaped);
        }

        // may not contain more than two escape-double-quote separators
        // 3 double quotes (> 2)
        let pat = r#"one\"two\"three\"four"#;
        let mut writer = String::new();
        let res = similar_to_escape_default(pat, &mut writer);
        assert!(res.is_err());
    }

    #[test]
    fn test_escape_with_escape_text() {
        let cases = vec![
            ("", "^(?:)$"),
            ("_bcd%", "^(?:.bcd.*)$"),
            ("bcd%", "^(?:bcd.*)$"),
            (r#"_bcd\%"#, r#"^(?:.bcd\\.*)$"#),
            ("bcd[]ee", "^(?:bcd[]ee)$"),
            (r#"bcd[]ee"""#, r#"^(?:bcd[]ee"")$"#),
            (r#"bcd[]"ee""#, r#"^(?:bcd[]"ee")$"#),
            ("bcd[pp]ee", "^(?:bcd[pp]ee)$"),
            ("bcd[pp_%.]ee", "^(?:bcd[pp_%.]ee)$"),
            ("bcd[pp_%.]ee_%.", r#"^(?:bcd[pp_%.]ee..*\.)$"#),
            ("bcd[pp_%.](ee_%.)", r#"^(?:bcd[pp_%.](?:ee..*\.))$"#),
            (r#"%#"o_b#"%"#, "^(?:.*){1,1}?(o.b){1,1}(?:.*)$"),
        ];

        for (pat, escaped) in cases {
            let mut writer = String::new();
            similar_to_escape_with_escape_text(pat, "#", &mut writer).ok();
            assert_eq!(writer, escaped);
        }

        let pat = "xxx";
        let mut writer = String::new();
        let res = similar_to_escape_with_escape_text(pat, "##", &mut writer);
        assert!(res.is_err())
    }

    #[test]
    fn test_escape_with_escape_unicode() {
        let cases = vec![
            ("", "^(?:)$"),
            ("_bcd%", "^(?:.bcd.*)$"),
            ("bcd%", "^(?:bcd.*)$"),
            (r#"_bcd\%"#, r#"^(?:.bcd\\.*)$"#),
            ("bcd[]ee", "^(?:bcd[]ee)$"),
            (r#"bcd[]ee"""#, r#"^(?:bcd[]ee"")$"#),
            (r#"bcd[]"ee""#, r#"^(?:bcd[]"ee")$"#),
            ("bcd[pp]ee", "^(?:bcd[pp]ee)$"),
            ("bcd[pp_%.]ee", "^(?:bcd[pp_%.]ee)$"),
            ("bcd[pp_%.]ee_%.", r#"^(?:bcd[pp_%.]ee..*\.)$"#),
            ("bcd[pp_%.](ee_%.)", r#"^(?:bcd[pp_%.](?:ee..*\.))$"#),
            (r#"%💅"o_b💅"%"#, "^(?:.*){1,1}?(o.b){1,1}(?:.*)$"),
        ];

        for (pat, escaped) in cases {
            let mut writer = String::new();
            similar_to_escape_with_escape_text(pat, "💅", &mut writer).ok();
            assert_eq!(writer, escaped);
        }

        let pat = "xxx";
        let mut writer = String::new();
        let res = similar_to_escape_with_escape_text(pat, "💅💅", &mut writer);
        assert!(res.is_err())
    }

    #[test]
    fn test_escape_with_escape_disabled() {
        let cases = vec![
            ("", "^(?:)$"),
            ("_bcd%", "^(?:.bcd.*)$"),
            ("bcd%", "^(?:bcd.*)$"),
            (r#"_bcd\%"#, r#"^(?:.bcd\\.*)$"#),
            ("bcd[]ee", "^(?:bcd[]ee)$"),
            (r#"bcd[]ee"""#, r#"^(?:bcd[]ee"")$"#),
            (r#"bcd[]"ee""#, r#"^(?:bcd[]"ee")$"#),
            ("bcd[pp]ee", "^(?:bcd[pp]ee)$"),
            ("bcd[pp_%.]ee", "^(?:bcd[pp_%.]ee)$"),
            ("bcd[pp_%.]ee_%.", r#"^(?:bcd[pp_%.]ee..*\.)$"#),
            ("bcd[pp_%.](ee_%.)", r#"^(?:bcd[pp_%.](?:ee..*\.))$"#),
            (r#"%\"o_b\"%"#, r#"^(?:.*\\"o.b\\".*)$"#),
        ];

        for (pat, escaped) in cases {
            let mut writer = String::new();
            similar_to_escape_with_escape_text(pat, "", &mut writer).ok();
            assert_eq!(writer, escaped);
        }
    }
}
