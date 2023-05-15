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

use std::borrow::Cow;

const PARSE_ERROR_STR_TO_BYTEA: &str = "Invalid Bytea syntax";

#[inline(always)]
pub fn str_to_bytea(elem: &str) -> Result<Box<[u8]>, Cow<'static, str>> {
    // Padded with whitespace str is not allowed.
    if elem.starts_with(' ') && elem.trim().starts_with("\\x") {
        Err(PARSE_ERROR_STR_TO_BYTEA.into())
    } else if let Some(remainder) = elem.strip_prefix(r"\x") {
        Ok(parse_bytes_hex(remainder)?.into())
    } else {
        Ok(parse_bytes_traditional(elem)?.into())
    }
}

// Refer to Materialize: https://github.com/MaterializeInc/materialize/blob/1766ab3978bc90abf75eb9b1fbadfcc95eca1993/src/repr/src/strconv.rs#L623
pub fn parse_bytes_hex(s: &str) -> Result<Vec<u8>, Cow<'static, str>> {
    // Can't use `hex::decode` here, as it doesn't tolerate whitespace
    // between encoded bytes.

    let decode_nibble = |b| match b {
        b'a'..=b'f' => Ok(b - b'a' + 10),
        b'A'..=b'F' => Ok(b - b'A' + 10),
        b'0'..=b'9' => Ok(b - b'0'),
        _ => Err(PARSE_ERROR_STR_TO_BYTEA),
    };

    let mut buf = vec![];
    let mut nibbles = s.as_bytes().iter().copied();
    while let Some(n) = nibbles.next() {
        if let b' ' | b'\n' | b'\t' | b'\r' = n {
            continue;
        }
        let n = decode_nibble(n)?;
        let n2 = match nibbles.next() {
            None => return Err(PARSE_ERROR_STR_TO_BYTEA.into()),
            Some(n2) => decode_nibble(n2)?,
        };
        buf.push((n << 4) | n2);
    }
    Ok(buf)
}

// Refer to https://github.com/MaterializeInc/materialize/blob/1766ab3978bc90abf75eb9b1fbadfcc95eca1993/src/repr/src/strconv.rs#L650
pub fn parse_bytes_traditional(s: &str) -> Result<Vec<u8>, Cow<'static, str>> {
    // Bytes are interpreted literally, save for the special escape sequences
    // "\\", which represents a single backslash, and "\NNN", where each N
    // is an octal digit, which represents the byte whose octal value is NNN.
    let mut out = Vec::new();
    let mut bytes = s.as_bytes().iter().fuse();
    while let Some(&b) = bytes.next() {
        if b != b'\\' {
            out.push(b);
            continue;
        }
        match bytes.next() {
            None => return Err(PARSE_ERROR_STR_TO_BYTEA.into()),
            Some(b'\\') => out.push(b'\\'),
            b => match (b, bytes.next(), bytes.next()) {
                (Some(d2 @ b'0'..=b'3'), Some(d1 @ b'0'..=b'7'), Some(d0 @ b'0'..=b'7')) => {
                    out.push(((d2 - b'0') << 6) + ((d1 - b'0') << 3) + (d0 - b'0'));
                }
                _ => return Err(PARSE_ERROR_STR_TO_BYTEA.into()),
            },
        }
    }
    Ok(out)
}

#[cfg(test)]
mod test {
    use crate::types::bytea::str_to_bytea;
    use crate::types::to_text::ToText;

    #[test]
    fn test_bytea() {
        assert_eq!(str_to_bytea("fgo").unwrap().as_ref().to_text(), r"\x66676f");
        assert_eq!(
            str_to_bytea(r"\xDeadBeef").unwrap().as_ref().to_text(),
            r"\xdeadbeef"
        );
        assert_eq!(
            str_to_bytea("12CD").unwrap().as_ref().to_text(),
            r"\x31324344"
        );
        assert_eq!(
            str_to_bytea("1234").unwrap().as_ref().to_text(),
            r"\x31323334"
        );
        assert_eq!(
            str_to_bytea(r"\x12CD").unwrap().as_ref().to_text(),
            r"\x12cd"
        );
        assert_eq!(
            str_to_bytea(r"\x De Ad Be Ef ").unwrap().as_ref().to_text(),
            r"\xdeadbeef"
        );
        assert_eq!(
            str_to_bytea("x De Ad Be Ef ").unwrap().as_ref().to_text(),
            r"\x7820446520416420426520456620"
        );
        assert_eq!(
            str_to_bytea(r"De\\123dBeEf").unwrap().as_ref().to_text(),
            r"\x44655c3132336442654566"
        );
        assert_eq!(
            str_to_bytea(r"De\123dBeEf").unwrap().as_ref().to_text(),
            r"\x4465536442654566"
        );
        assert_eq!(
            str_to_bytea(r"De\\000dBeEf").unwrap().as_ref().to_text(),
            r"\x44655c3030306442654566"
        );
    }
}
