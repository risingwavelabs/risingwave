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

use risingwave_expr_macro::function;

use crate::{ExprError, Result};

/// Replaces a substring of the given string with a new substring.
///
/// ```slt
/// query T
/// select overlay('αβγδεζ' placing '💯' from 3);
/// ----
/// αβ💯δεζ
/// ```
#[function("overlay(varchar, varchar, int32) -> varchar")]
pub fn overlay(s: &str, new_sub_str: &str, start: i32, writer: &mut dyn Write) -> Result<()> {
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
#[function("overlay(varchar, varchar, int32, int32) -> varchar")]
pub fn overlay_for(
    s: &str,
    new_sub_str: &str,
    start: i32,
    count: i32,
    writer: &mut dyn Write,
) -> Result<()> {
    if start <= 0 {
        return Err(ExprError::InvalidParam {
            name: "start",
            reason: format!("{start} is not positive"),
        });
    }

    let mut chars = s.chars();
    for _ in 1..start {
        if let Some(c) = chars.next() {
            writer.write_char(c).unwrap();
        }
    }

    writer.write_str(new_sub_str).unwrap();

    let Ok(count) = count.try_into() else {
        // For negative `count`, which is rare in practice, we hand over to `substr`
        let start_right = start
            .checked_add(count)
            .ok_or(ExprError::NumericOutOfRange)?;
        return super::substr::substr_start(s, start_right, writer);
    };

    for c in chars.skip(count) {
        writer.write_char(c).unwrap();
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_overlay() {
        let cases = vec![
            ("aaa__aaa", "XY", 4, None, "aaaXYaaa"),
            // Place at end.
            ("aaa", "XY", 4, None, "aaaXY"),
            // Place at start.
            ("aaa", "XY", 1, Some(0), "XYaaa"),
            // Replace shorter string.
            ("aaa_aaa", "XYZ", 4, Some(1), "aaaXYZaaa"),
            ("aaaaaa", "XYZ", 4, Some(0), "aaaXYZaaa"),
            // Replace longer string.
            ("aaa___aaa", "X", 4, Some(3), "aaaXaaa"),
            // start too large.
            ("aaa", "XY", 123, None, "aaaXY"),
            // count too small or large.
            ("aaa", "X", 4, Some(-123), "aaaXaaa"),
            ("aaa_", "X", 4, Some(123), "aaaX"),
        ];

        for (s, new_sub_str, start, count, expected) in cases {
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
