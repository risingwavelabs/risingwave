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

//! String functions
//!
//! <https://www.postgresql.org/docs/current/functions-string.html>

use std::fmt::Write;

use risingwave_common::util::quote_ident::QuoteIdent;
use risingwave_expr::function;

/// Returns the character with the specified Unicode code point.
///
/// # Example
///
/// ```slt
/// query T
/// select chr(65);
/// ----
/// A
/// ```
#[function("chr(int4) -> varchar")]
pub fn chr(code: i32, writer: &mut impl Write) {
    if let Some(c) = std::char::from_u32(code as u32) {
        write!(writer, "{}", c).unwrap();
    }
}

/// Returns true if the given string starts with the specified prefix.
///
/// # Example
///
/// ```slt
/// query T
/// select starts_with('abcdef', 'abc');
/// ----
/// t
///
/// query T
/// select 'abcdef' ^@ 'abc';
/// ----
/// t
///
/// query T
/// select 'abcdef' ^@ some(array['x', 'a', 't']);
/// ----
/// t
/// ```
#[function("starts_with(varchar, varchar) -> boolean")]
pub fn starts_with(s: &str, prefix: &str) -> bool {
    s.starts_with(prefix)
}

/// Capitalizes the first letter of each word in the given string.
///
/// # Example
///
/// ```slt
/// query T
/// select initcap('the quick brown fox');
/// ----
/// The Quick Brown Fox
/// ```
#[function("initcap(varchar) -> varchar")]
pub fn initcap(s: &str, writer: &mut impl Write) {
    let mut capitalize_next = true;
    for c in s.chars() {
        if capitalize_next {
            write!(writer, "{}", c.to_uppercase()).unwrap();
            capitalize_next = false;
        } else {
            write!(writer, "{}", c.to_lowercase()).unwrap();
        }
        if c.is_whitespace() {
            capitalize_next = true;
        }
    }
}

/// Extends the given string on the left until it is at least the specified length,
/// using the specified fill character (or a space by default).
///
/// # Example
///
/// ```slt
/// query T
/// select lpad('abc', 5);
/// ----
///   abc
///
/// query T
/// select lpad('abcdef', 3);
/// ----
/// abc
/// ```
#[function("lpad(varchar, int4) -> varchar")]
pub fn lpad(s: &str, length: i32, writer: &mut impl Write) {
    lpad_fill(s, length, " ", writer);
}

/// Extends the string to the specified length by prepending the characters fill.
/// If the string is already longer than the specified length, it is truncated on the right.
///
/// # Example
///
/// ```slt
/// query T
/// select lpad('hi', 5, 'xy');
/// ----
/// xyxhi
///
/// query T
/// select lpad('hi', 5, '');
/// ----
/// hi
/// ```
#[function("lpad(varchar, int4, varchar) -> varchar")]
pub fn lpad_fill(s: &str, length: i32, fill: &str, writer: &mut impl Write) {
    let s_len = s.chars().count();
    let fill_len = fill.chars().count();

    if length <= 0 {
        return;
    }
    if s_len >= length as usize {
        for c in s.chars().take(length as usize) {
            write!(writer, "{c}").unwrap();
        }
    } else if fill_len == 0 {
        write!(writer, "{s}").unwrap();
    } else {
        let mut remaining_length = length as usize - s_len;
        while remaining_length >= fill_len {
            write!(writer, "{fill}").unwrap();
            remaining_length -= fill_len;
        }
        for c in fill.chars().take(remaining_length) {
            write!(writer, "{c}").unwrap();
        }
        write!(writer, "{s}").unwrap();
    }
}

/// Extends the given string on the right until it is at least the specified length,
/// using the specified fill character (or a space by default).
///
/// # Example
///
/// ```slt
/// query T
/// select rpad('abc', 5);
/// ----
/// abc
///
/// query T
/// select rpad('abcdef', 3);
/// ----
/// abc
/// ```
#[function("rpad(varchar, int4) -> varchar")]
pub fn rpad(s: &str, length: i32, writer: &mut impl Write) {
    rpad_fill(s, length, " ", writer);
}

/// Extends the given string on the right until it is at least the specified length,
/// using the specified fill string, truncating the string if it is already longer
/// than the specified length.
///
/// # Example
///
/// ```slt
/// query T
/// select rpad('hi', 5, 'xy');
/// ----
/// hixyx
///
/// query T
/// select rpad('abc', 5, '😀');
/// ----
/// abc😀😀
///
/// query T
/// select rpad('abcdef', 3, '0');
/// ----
/// abc
///
/// query T
/// select rpad('hi', 5, '');
/// ----
/// hi
/// ```
#[function("rpad(varchar, int4, varchar) -> varchar")]
pub fn rpad_fill(s: &str, length: i32, fill: &str, writer: &mut impl Write) {
    let s_len = s.chars().count();
    let fill_len = fill.chars().count();

    if length <= 0 {
        return;
    }

    if s_len >= length as usize {
        for c in s.chars().take(length as usize) {
            write!(writer, "{c}").unwrap();
        }
    } else if fill_len == 0 {
        write!(writer, "{s}").unwrap();
    } else {
        write!(writer, "{s}").unwrap();
        let mut remaining_length = length as usize - s_len;
        while remaining_length >= fill_len {
            write!(writer, "{fill}").unwrap();
            remaining_length -= fill_len;
        }
        for c in fill.chars().take(remaining_length) {
            write!(writer, "{c}").unwrap();
        }
    }
}

/// Reverses the characters in the given string.
///
/// # Example
///
/// ```slt
/// query T
/// select reverse('abcdef');
/// ----
/// fedcba
/// ```
#[function("reverse(varchar) -> varchar")]
pub fn reverse(s: &str, writer: &mut impl Write) {
    for c in s.chars().rev() {
        write!(writer, "{}", c).unwrap();
    }
}

/// Converts the input string to ASCII by dropping accents, assuming that the input string
/// is encoded in one of the supported encodings (Latin1, Latin2, Latin9, or WIN1250).
///
/// # Example
///
/// ```slt
/// query T
/// select to_ascii('Karél');
/// ----
/// Karel
/// ```
#[function("to_ascii(varchar) -> varchar")]
pub fn to_ascii(s: &str, writer: &mut impl Write) {
    for c in s.chars() {
        let ascii = match c {
            'Á' | 'À' | 'Â' | 'Ã' => 'A',
            'á' | 'à' | 'â' | 'ã' => 'a',
            'Č' | 'Ć' | 'Ç' => 'C',
            'č' | 'ć' | 'ç' => 'c',
            'Ď' => 'D',
            'ď' => 'd',
            'É' | 'È' | 'Ê' | 'Ẽ' => 'E',
            'é' | 'è' | 'ê' | 'ẽ' => 'e',
            'Í' | 'Ì' | 'Î' | 'Ĩ' => 'I',
            'í' | 'ì' | 'î' | 'ĩ' => 'i',
            'Ľ' => 'L',
            'ľ' => 'l',
            'Ň' => 'N',
            'ň' => 'n',
            'Ó' | 'Ò' | 'Ô' | 'Õ' => 'O',
            'ó' | 'ò' | 'ô' | 'õ' => 'o',
            'Ŕ' => 'R',
            'ŕ' => 'r',
            'Š' | 'Ś' => 'S',
            'š' | 'ś' => 's',
            'Ť' => 'T',
            'ť' => 't',
            'Ú' | 'Ù' | 'Û' | 'Ũ' => 'U',
            'ú' | 'ù' | 'û' | 'ũ' => 'u',
            'Ý' | 'Ỳ' => 'Y',
            'ý' | 'ỳ' => 'y',
            'Ž' | 'Ź' | 'Ż' => 'Z',
            'ž' | 'ź' | 'ż' => 'z',
            _ => c,
        };
        write!(writer, "{}", ascii).unwrap();
    }
}

/// Converts the given integer to its equivalent hexadecimal representation.
///
/// # Example
///
/// ```slt
/// query T
/// select to_hex(2147483647);
/// ----
/// 7fffffff
///
/// query T
/// select to_hex(-2147483648);
/// ----
/// 80000000
///
/// query T
/// select to_hex(9223372036854775807);
/// ----
/// 7fffffffffffffff
///
/// query T
/// select to_hex(-9223372036854775808);
/// ----
/// 8000000000000000
/// ```
#[function("to_hex(int4) -> varchar")]
pub fn to_hex_i32(n: i32, writer: &mut impl Write) {
    write!(writer, "{:x}", n).unwrap();
}

#[function("to_hex(int8) -> varchar")]
pub fn to_hex_i64(n: i64, writer: &mut impl Write) {
    write!(writer, "{:x}", n).unwrap();
}

/// Returns the given string suitably quoted to be used as an identifier in an SQL statement string.
/// Quotes are added only if necessary (i.e., if the string contains non-identifier characters or
/// would be case-folded). Embedded quotes are properly doubled.
///
/// Refer to <https://github.com/postgres/postgres/blob/90189eefc1e11822794e3386d9bafafd3ba3a6e8/src/backend/utils/adt/ruleutils.c#L11506>
///
/// # Example
///
/// ```slt
/// query T
/// select quote_ident('foo bar')
/// ----
/// "foo bar"
///
/// query T
/// select quote_ident('FooBar')
/// ----
/// "FooBar"
///
/// query T
/// select quote_ident('foo_bar')
/// ----
/// foo_bar
///
/// query T
/// select quote_ident('foo"bar')
/// ----
/// "foo""bar"
///
/// # FIXME: quote SQL keywords is not supported yet
/// query T
/// select quote_ident('select')
/// ----
/// select
/// ```
#[function("quote_ident(varchar) -> varchar")]
pub fn quote_ident(s: &str, writer: &mut impl Write) {
    write!(writer, "{}", QuoteIdent(s)).unwrap();
}

/// Returns the first n characters in the string.
/// If n is a negative value, the function will return all but last |n| characters.
///
/// # Example
///
/// ```slt
/// query T
/// select left('RisingWave', 6)
/// ----
/// Rising
///
/// query T
/// select left('RisingWave', 42)
/// ----
/// RisingWave
///
/// query T
/// select left('RisingWave', 0)
/// ----
/// (empty)
///
/// query T
/// select left('RisingWave', -4)
/// ----
/// Rising
///
/// query T
/// select left('RisingWave', -2147483648);
/// ----
/// (empty)
/// ```
#[function("left(varchar, int4) -> varchar")]
pub fn left(s: &str, n: i32, writer: &mut impl Write) {
    let n = if n >= 0 {
        n as usize
    } else {
        s.chars().count().saturating_add_signed(n as isize)
    };

    s.chars()
        .take(n)
        .for_each(|c| writer.write_char(c).unwrap());
}

/// Returns the last n characters in the string.
/// If n is a negative value, the function will return all but first |n| characters.
///
/// # Example
///
/// ```slt
/// query T
/// select right('RisingWave', 4)
/// ----
/// Wave
///
/// query T
/// select left('RisingWave', 42)
/// ----
/// RisingWave
///
/// query T
/// select right('RisingWave', 0)
/// ----
/// (empty)
///
/// query T
/// select right('RisingWave', -6)
/// ----
/// Wave
///
/// # PostgreSQL returns the whole string due to an overflow bug, which we do not follow.
/// query T
/// select right('RisingWave', -2147483648);
/// ----
/// (empty)
/// ```
#[function("right(varchar, int4) -> varchar")]
pub fn right(s: &str, n: i32, writer: &mut impl Write) {
    let skip = if n >= 0 {
        s.chars().count().saturating_sub(n as usize)
    } else {
        // `n as usize` is signed extended. This is `-n` without overflow.
        usize::MAX - (n as usize) + 1
    };

    s.chars()
        .skip(skip)
        .for_each(|c| writer.write_char(c).unwrap());
}

/// `quote_literal(string text)`
/// `quote_literal(value anyelement)`
///
/// Return the given string suitably quoted to be used as a string literal in an SQL statement
/// string. Embedded single-quotes and backslashes are properly doubled.
/// Note that `quote_literal` returns null on null input; if the argument might be null,
/// `quote_nullable` is often more suitable.
///
/// # Example
///
/// Note that the quotes are part of the output string.
///
/// ```slt
/// query T
/// select quote_literal(E'O\'Reilly')
/// ----
/// 'O''Reilly'
///
/// query T
/// select quote_literal(E'C:\\Windows\\')
/// ----
/// E'C:\\Windows\\'
///
/// query T
/// select quote_literal(42.5)
/// ----
/// '42.5'
///
/// query T
/// select quote_literal('hello'::bytea);
/// ----
/// E'\\x68656c6c6f'
///
/// query T
/// select quote_literal('{"hello":"world","foo":233}'::jsonb);
/// ----
/// '{"foo": 233, "hello": "world"}'
/// ```
#[function("quote_literal(varchar) -> varchar")]
pub fn quote_literal(s: &str, writer: &mut impl Write) {
    if s.contains('\\') {
        // use escape format: E'...'
        write!(writer, "E").unwrap();
    }
    write!(writer, "'").unwrap();
    for c in s.chars() {
        match c {
            '\'' => write!(writer, "''").unwrap(),
            '\\' => write!(writer, "\\\\").unwrap(),
            _ => write!(writer, "{}", c).unwrap(),
        }
    }
    write!(writer, "'").unwrap();
}

/// `quote_nullable(string text)`
///
/// Return the given string suitably quoted to be used as a string literal in an SQL statement
/// string; or, if the argument is null, return NULL.
/// Embedded single-quotes and backslashes are properly doubled.
#[function("quote_nullable(varchar) -> varchar")]
pub fn quote_nullable(s: Option<&str>, writer: &mut impl Write) {
    match s {
        Some(s) => quote_literal(s, writer),
        None => write!(writer, "NULL").unwrap(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_left_and_right() {
        let s = "cxscgccdd";
        let us = "上海自来水来自海上";

        let cases = [
            (s, 3, "cxs", "cdd"),
            (s, -3, "cxscgc", "cgccdd"),
            (s, 0, "", ""),
            (s, 15, "cxscgccdd", "cxscgccdd"),
            // Unicode test
            (us, 5, "上海自来水", "水来自海上"),
            (us, -6, "上海自", "自海上"),
        ];

        for (s, n, left_expected, right_expected) in cases {
            let mut left_writer = String::new();
            let mut right_writer = String::new();
            left(s, n, &mut left_writer);
            right(s, n, &mut right_writer);
            assert_eq!(left_writer, left_expected);
            assert_eq!(right_writer, right_expected);
        }
    }
}
