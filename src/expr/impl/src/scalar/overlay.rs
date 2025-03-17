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

/// Replaces a substring of the given string with a new substring.
///
/// ```slt
/// query T
/// select overlay('αβγδεζ' placing '💯' from 3);
/// ----
/// αβ💯δεζ
/// ```
#[function("overlay(varchar, varchar, int4) -> varchar")]
pub fn overlay(s: &str, new_sub_str: &str, start: i32, writer: &mut impl Write) -> Result<()> {
    let sub_len = new_sub_str
        .chars()
        .count()
        .try_into()
        .map_err(|_| ExprError::NumericOutOfRange)?;
    overlay_for(s, new_sub_str, start, sub_len, writer)
}

/// Replaces a substring of the given string with a new substring.
///
/// ```slt
/// statement error not positive
/// select overlay('αβγδεζ' placing '①②③' from 0);
///
/// query T
/// select overlay('αβγδεζ' placing '①②③' from 10);
/// ----
/// αβγδεζ①②③
///
/// query T
/// select overlay('αβγδεζ' placing '①②③' from 4 for 2);
/// ----
/// αβγ①②③ζ
///
/// query T
/// select overlay('αβγδεζ' placing '①②③' from 4);
/// ----
/// αβγ①②③
///
/// query T
/// select overlay('αβγδεζ' placing '①②③' from 2 for 4);
/// ----
/// α①②③ζ
///
/// query T
/// select overlay('αβγδεζ' placing '①②③' from 2 for 7);
/// ----
/// α①②③
///
/// query T
/// select overlay('αβγδεζ' placing '①②③' from 4 for 0);
/// ----
/// αβγ①②③δεζ
///
/// query T
/// select overlay('αβγδεζ' placing '①②③' from 4 for -2);
/// ----
/// αβγ①②③βγδεζ
///
/// query T
/// select overlay('αβγδεζ' placing '①②③' from 4 for -1000);
/// ----
/// αβγ①②③αβγδεζ
/// ```
#[function("overlay(varchar, varchar, int4, int4) -> varchar")]
pub fn overlay_for(
    s: &str,
    new_sub_str: &str,
    start: i32,
    count: i32,
    writer: &mut impl Write,
) -> Result<()> {
    if start <= 0 {
        return Err(ExprError::InvalidParam {
            name: "start",
            reason: format!("{start} is not positive").into(),
        });
    }

    let mut chars = s.char_indices().skip(start as usize - 1).peekable();

    // write the substring before the overlay.
    let leading = match chars.peek() {
        Some((i, _)) => &s[..*i],
        None => s,
    };
    writer.write_str(leading).unwrap();

    // write the new substring.
    writer.write_str(new_sub_str).unwrap();

    let Ok(count) = count.try_into() else {
        // For negative `count`, which is rare in practice, we hand over to `substr`
        let start_right = start
            .checked_add(count)
            .ok_or(ExprError::NumericOutOfRange)?;
        return super::substr::substr_start(s, start_right, writer);
    };

    // write the substring after the overlay.
    if let Some((i, _)) = chars.nth(count) {
        writer.write_str(&s[i..]).unwrap();
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_overlay() {
        case("aaa__aaa", "XY", 4, None, "aaaXYaaa");
        // Place at end.
        case("aaa", "XY", 4, None, "aaaXY");
        // Place at start.
        case("aaa", "XY", 1, Some(0), "XYaaa");
        // Replace shorter string.
        case("aaa_aaa", "XYZ", 4, Some(1), "aaaXYZaaa");
        case("aaaaaa", "XYZ", 4, Some(0), "aaaXYZaaa");
        // Replace longer string.
        case("aaa___aaa", "X", 4, Some(3), "aaaXaaa");
        // start too large.
        case("aaa", "XY", 123, None, "aaaXY");
        // count too small or large.
        case("aaa", "X", 4, Some(-123), "aaaXaaa");
        case("aaa_", "X", 4, Some(123), "aaaX");
        // very large start and count
        case("aaa", "X", i32::MAX, Some(i32::MAX), "aaaX");

        #[track_caller]
        fn case(s: &str, new_sub_str: &str, start: i32, count: Option<i32>, expected: &str) {
            let mut writer = String::new();
            match count {
                None => overlay(s, new_sub_str, start, &mut writer),
                Some(count) => overlay_for(s, new_sub_str, start, count, &mut writer),
            }
            .unwrap();
            assert_eq!(writer, expected);
        }
    }
}
