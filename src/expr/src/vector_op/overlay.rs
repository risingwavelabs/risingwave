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

#[function("overlay(varchar, varchar, int32) -> varchar")]
pub fn overlay(s: &str, new_sub_str: &str, start: i32, writer: &mut dyn Write) -> Result<()> {
    // If count is omitted, it defaults to the length of new_sub_str.
    overlay_for(s, new_sub_str, start, new_sub_str.len() as i32, writer)
}

#[function("overlay(varchar, varchar, int32, int32) -> varchar")]
pub fn overlay_for(
    s: &str,
    new_sub_str: &str,
    start: i32,
    count: i32,
    writer: &mut dyn Write,
) -> Result<()> {
    let count = count.max(0) as usize;

    // If start is out of range, attach it to the end.
    // Note that indices are 1-based.
    let start = (start
        .checked_sub(1)
        .ok_or(ExprError::NumericOutOfRange)?
        .max(0) as usize)
        .min(s.len());

    let remaining = start + count;

    writer.write_str(&s[..start]).unwrap();
    writer.write_str(new_sub_str).unwrap();

    if remaining < s.len() {
        writer.write_str(&s[remaining..]).unwrap();
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
            // start too small or large.
            ("aaa", "XY", -123, None, "XYa"),
            ("aaa", "XY", 123, None, "aaaXY"),
            // count too small or large.
            ("aaa", "X", 4, Some(-123), "aaaX"),
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
