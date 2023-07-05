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

use hex;
use risingwave_common::cast::{parse_bytes_hex, parse_bytes_traditional};
use risingwave_expr_macro::function;

use crate::{ExprError, Result};

const PARSE_BASE64_INVALID_END: &str = "invalid base64 end sequence";
const PARSE_BASE64_INVALID_PADDING: &str = "unexpected \"=\" while decoding base64 sequence";
const PARSE_BASE64_ALPHABET: &[u8] =
    b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
const PARSE_BASE64_IGNORE_BYTES: [u8; 4] = [0x0D, 0x0A, 0x20, 0x09];
// such as  'A'/0x41 -> 0  'B'/0x42 -> 1
const PARSE_BASE64_ALPHABET_DECODE_TABLE: [u8; 123] = [
    0x7f, 0x7f, 0x7f, 0x7f, 0x7f, 0x7f, 0x7f, 0x7f, 0x7f, 0x7f, 0x7f, 0x7f, 0x7f, 0x7f, 0x7f, 0x7f,
    0x7f, 0x7f, 0x7f, 0x7f, 0x7f, 0x7f, 0x7f, 0x7f, 0x7f, 0x7f, 0x7f, 0x7f, 0x7f, 0x7f, 0x7f, 0x7f,
    0x7f, 0x7f, 0x7f, 0x7f, 0x7f, 0x7f, 0x7f, 0x7f, 0x7f, 0x7f, 0x3E, 0x7f, 0x7f, 0x7f, 0x7f, 0x3F,
    0x34, 0x35, 0x36, 0x37, 0x38, 0x39, 0x3A, 0x3B, 0x3C, 0x3D, 0x7f, 0x7f, 0x7f, 0x7f, 0x7f, 0x7f,
    0x7f, 0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E,
    0x0F, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x7f, 0x7f, 0x7f, 0x7f, 0x7f,
    0x7f, 0x1A, 0x1B, 0x1C, 0x1D, 0x1E, 0x1F, 0x20, 0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28,
    0x29, 0x2A, 0x2B, 0x2C, 0x2D, 0x2E, 0x2F, 0x30, 0x31, 0x32, 0x33,
];

#[function("encode(bytea, varchar) -> varchar")]
pub fn encode(data: &[u8], format: &str, writer: &mut dyn Write) -> Result<()> {
    match format {
        "base64" => {
            encode_bytes_base64(data, writer)?;
        }
        "hex" => {
            writer.write_str(&hex::encode(data)).unwrap();
        }
        "escape" => {
            encode_bytes_escape(data, writer).unwrap();
        }
        _ => {
            return Err(ExprError::InvalidParam {
                name: "format",
                reason: format!("unrecognized encoding: \"{}\"", format),
            });
        }
    }
    Ok(())
}

#[function("decode(varchar, varchar) -> bytea")]
pub fn decode(data: &str, format: &str) -> Result<Box<[u8]>> {
    match format {
        "base64" => Ok(parse_bytes_base64(data)?.into()),
        "hex" => Ok(parse_bytes_hex(data)
            .map_err(|err| ExprError::Parse(err.into()))?
            .into()),
        "escape" => Ok(parse_bytes_traditional(data)
            .map_err(|err| ExprError::Parse(err.into()))?
            .into()),
        _ => Err(ExprError::InvalidParam {
            name: "format",
            reason: format!("unrecognized encoding: \"{}\"", format),
        }),
    }
}

// According to https://www.postgresql.org/docs/current/functions-binarystring.html#ENCODE-FORMAT-BASE64
// We need to split newlines when the output length is greater than or equal to 76
fn encode_bytes_base64(data: &[u8], writer: &mut dyn Write) -> Result<()> {
    let mut idx: usize = 0;
    let len = data.len();
    let mut written = 0;
    while idx + 2 < len {
        let i1 = (data[idx] >> 2) & 0b00111111;
        let i2 = ((data[idx] & 0b00000011) << 4) | ((data[idx + 1] >> 4) & 0b00001111);
        let i3 = ((data[idx + 1] & 0b00001111) << 2) | ((data[idx + 2] >> 6) & 0b00000011);
        let i4 = data[idx + 2] & 0b00111111;
        writer
            .write_char(PARSE_BASE64_ALPHABET[usize::from(i1)].into())
            .unwrap();
        writer
            .write_char(PARSE_BASE64_ALPHABET[usize::from(i2)].into())
            .unwrap();
        writer
            .write_char(PARSE_BASE64_ALPHABET[usize::from(i3)].into())
            .unwrap();
        writer
            .write_char(PARSE_BASE64_ALPHABET[usize::from(i4)].into())
            .unwrap();

        written += 4;
        idx += 3;
        if written % 76 == 0 {
            writer.write_char('\n').unwrap();
        }
    }

    if idx + 2 == len {
        let i1 = (data[idx] >> 2) & 0b00111111;
        let i2 = ((data[idx] & 0b00000011) << 4) | ((data[idx + 1] >> 4) & 0b00001111);
        let i3 = (data[idx + 1] & 0b00001111) << 2;
        writer
            .write_char(PARSE_BASE64_ALPHABET[usize::from(i1)].into())
            .unwrap();
        writer
            .write_char(PARSE_BASE64_ALPHABET[usize::from(i2)].into())
            .unwrap();
        writer
            .write_char(PARSE_BASE64_ALPHABET[usize::from(i3)].into())
            .unwrap();
        writer.write_char('=').unwrap();
    } else if idx + 1 == len {
        let i1 = (data[idx] >> 2) & 0b00111111;
        let i2 = (data[idx] & 0b00000011) << 4;
        writer
            .write_char(PARSE_BASE64_ALPHABET[usize::from(i1)].into())
            .unwrap();
        writer
            .write_char(PARSE_BASE64_ALPHABET[usize::from(i2)].into())
            .unwrap();
        writer.write_char('=').unwrap();
        writer.write_char('=').unwrap();
    }
    Ok(())
}

// According to https://www.postgresql.org/docs/current/functions-binarystring.html#ENCODE-FORMAT-BASE64
// parse_bytes_base64 need ignores carriage-return[0x0D], newline[0x0A], space[0x20], and tab[0x09].
// When decode is supplied invalid base64 data, including incorrect trailing padding, return error.
fn parse_bytes_base64(data: &str) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    let data_bytes = data.as_bytes();

    let mut idx: usize = 0;
    while idx < data.len() {
        match (
            next(&mut idx, data_bytes),
            next(&mut idx, data_bytes),
            next(&mut idx, data_bytes),
            next(&mut idx, data_bytes),
        ) {
            (None, None, None, None) => return Ok(out),
            (Some(d1), Some(d2), Some(b'='), Some(b'=')) => {
                let s1 = alphabet_decode(d1)?;
                let s2 = alphabet_decode(d2)?;
                out.push(s1 << 2 | s2 >> 4);
            }
            (Some(d1), Some(d2), Some(d3), Some(b'=')) => {
                let s1 = alphabet_decode(d1)?;
                let s2 = alphabet_decode(d2)?;
                let s3 = alphabet_decode(d3)?;
                out.push(s1 << 2 | s2 >> 4);
                out.push(s2 << 4 | s3 >> 2);
            }
            (Some(b'='), _, _, _) => {
                return Err(ExprError::Parse(PARSE_BASE64_INVALID_PADDING.into()));
            }
            (Some(d1), Some(b'='), _, _) => {
                alphabet_decode(d1)?;
                return Err(ExprError::Parse(PARSE_BASE64_INVALID_PADDING.into()));
            }
            (Some(d1), Some(d2), Some(b'='), _) => {
                alphabet_decode(d1)?;
                alphabet_decode(d2)?;
                return Err(ExprError::Parse(PARSE_BASE64_INVALID_PADDING.into()));
            }
            (Some(d1), Some(d2), Some(d3), Some(d4)) => {
                let s1 = alphabet_decode(d1)?;
                let s2 = alphabet_decode(d2)?;
                let s3 = alphabet_decode(d3)?;
                let s4 = alphabet_decode(d4)?;
                out.push(s1 << 2 | s2 >> 4);
                out.push(s2 << 4 | s3 >> 2);
                out.push(s3 << 6 | s4);
            }
            (Some(d1), None, None, None) => {
                alphabet_decode(d1)?;
                return Err(ExprError::Parse(PARSE_BASE64_INVALID_END.into()));
            }
            (Some(d1), Some(d2), None, None) => {
                alphabet_decode(d1)?;
                alphabet_decode(d2)?;
                return Err(ExprError::Parse(PARSE_BASE64_INVALID_END.into()));
            }
            (Some(d1), Some(d2), Some(d3), None) => {
                alphabet_decode(d1)?;
                alphabet_decode(d2)?;
                alphabet_decode(d3)?;
                return Err(ExprError::Parse(PARSE_BASE64_INVALID_END.into()));
            }
            _ => {
                return Err(ExprError::Parse(PARSE_BASE64_INVALID_END.into()));
            }
        }
    }
    Ok(out)
}

#[inline]
fn alphabet_decode(d: u8) -> Result<u8> {
    if d > 0x7A {
        Err(ExprError::Parse(
            format!(
                "invalid symbol \"{}\" while decoding base64 sequence",
                std::char::from_u32(d as u32).unwrap()
            )
            .into(),
        ))
    } else {
        let p = PARSE_BASE64_ALPHABET_DECODE_TABLE[d as usize];
        if p == 0x7f {
            Err(ExprError::Parse(
                format!(
                    "invalid symbol \"{}\" while decoding base64 sequence",
                    std::char::from_u32(d as u32).unwrap()
                )
                .into(),
            ))
        } else {
            Ok(p)
        }
    }
}

#[inline]
fn next(idx: &mut usize, data: &[u8]) -> Option<u8> {
    while *idx < data.len() && PARSE_BASE64_IGNORE_BYTES.contains(&data[*idx]) {
        *idx += 1;
    }
    if *idx < data.len() {
        let d1 = data[*idx];
        *idx += 1;
        Some(d1)
    } else {
        None
    }
}

// According to https://www.postgresql.org/docs/current/functions-binarystring.html#ENCODE-FORMAT-ESCAPE
// The escape format converts \0 and bytes with the high bit set into octal escape sequences (\nnn).
// And doubles backslashes.
fn encode_bytes_escape(data: &[u8], writer: &mut dyn Write) -> std::fmt::Result {
    for b in data {
        match b {
            b'\0' | (b'\x80'..=b'\xff') => {
                write!(writer, "\\{:03o}", b).unwrap();
            }
            b'\\' => writer.write_str("\\\\")?,
            _ => writer.write_char((*b).into())?,
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{decode, encode};

    #[test]
    fn test_encdec() {
        let cases = [
            (r#"ABCDE"#.as_bytes(), "base64", r#"QUJDREU="#.as_bytes()),
            (r#"\""#.as_bytes(), "escape", r#"\\""#.as_bytes()),
            (
                b"\x00\x40\x41\x42\xff",
                "escape",
                r#"\000@AB\377"#.as_bytes(),
            ),
            (
                "aaaaaaaaaabbbbbbbbbbccccccccccddddddddddeeeeeeeeeefffffff".as_bytes(),
                "base64",
                "YWFhYWFhYWFhYWJiYmJiYmJiYmJjY2NjY2NjY2NjZGRkZGRkZGRkZGVlZWVlZWVlZWVmZmZmZmZm\n"
                    .as_bytes(),
            ),
            (
                "aabbccddee".as_bytes(),
                "hex",
                "61616262636364646565".as_bytes(),
            ),
        ];

        for (ori, format, encoded) in cases {
            let mut w = String::new();
            assert!(encode(ori, format, &mut w).is_ok());
            println!("{}", w);
            assert_eq!(w.as_bytes(), encoded);
            let res = decode(w.as_str(), format).unwrap();
            assert_eq!(ori, res.as_ref());
        }
    }
}
