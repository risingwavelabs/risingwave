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

#[function("split_part(varchar, varchar, int4) -> varchar")]
pub fn split_part(
    string_expr: &str,
    delimiter_expr: &str,
    nth_expr: i32,
    writer: &mut impl Write,
) -> Result<()> {
    if nth_expr == 0 {
        return Err(ExprError::InvalidParam {
            name: "data",
            reason: "can't be zero".into(),
        });
    };

    let mut split = string_expr.split(delimiter_expr);
    let nth_val = if string_expr.is_empty() {
        // postgres: return empty string for empty input string
        Default::default()
    } else if delimiter_expr.is_empty() {
        // postgres: handle empty field separator
        //           if first or last field, return input string, else empty string
        if nth_expr == 1 || nth_expr == -1 {
            string_expr
        } else {
            Default::default()
        }
    } else {
        match nth_expr.cmp(&0) {
            std::cmp::Ordering::Equal => unreachable!(),

            // Since `nth_expr` can not be 0, so the `abs()` of it can not be smaller than 1
            // (that's `abs(1)` or `abs(-1)`).  Hence the result of sub 1 can not be less than 0.
            // postgres: if nonexistent field, return empty string
            std::cmp::Ordering::Greater => split.nth(nth_expr as usize - 1).unwrap_or_default(),
            std::cmp::Ordering::Less => {
                let split = split.collect::<Vec<_>>();
                split
                    .iter()
                    .rev()
                    .nth(nth_expr.unsigned_abs() as usize - 1)
                    .cloned()
                    .unwrap_or_default()
            }
        }
    };
    writer.write_str(nth_val).unwrap();
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::split_part;

    #[test]
    fn test_split_part() {
        let cases: Vec<(&str, &str, i32, Option<&str>)> = vec![
            // postgres cases
            ("", "@", 1, Some("")),
            ("", "@", -1, Some("")),
            ("joeuser@mydatabase", "", 1, Some("joeuser@mydatabase")),
            ("joeuser@mydatabase", "", 2, Some("")),
            ("joeuser@mydatabase", "", -1, Some("joeuser@mydatabase")),
            ("joeuser@mydatabase", "", -2, Some("")),
            ("joeuser@mydatabase", "@", 0, None),
            ("joeuser@mydatabase", "@@", 1, Some("joeuser@mydatabase")),
            ("joeuser@mydatabase", "@@", 2, Some("")),
            ("joeuser@mydatabase", "@", 1, Some("joeuser")),
            ("joeuser@mydatabase", "@", 2, Some("mydatabase")),
            ("joeuser@mydatabase", "@", 3, Some("")),
            ("@joeuser@mydatabase@", "@", 2, Some("joeuser")),
            ("joeuser@mydatabase", "@", -1, Some("mydatabase")),
            ("joeuser@mydatabase", "@", -2, Some("joeuser")),
            ("joeuser@mydatabase", "@", -3, Some("")),
            ("@joeuser@mydatabase@", "@", -2, Some("mydatabase")),
            // other cases

            // makes sure that `rsplit` is not used internally when `nth` is negative
            ("@@@", "@@", -1, Some("@")),
        ];

        for (i, case @ (string_expr, delimiter_expr, nth_expr, expected)) in
            cases.iter().enumerate()
        {
            let mut writer = String::new();
            let actual = match split_part(string_expr, delimiter_expr, *nth_expr, &mut writer) {
                Ok(_) => Some(writer.as_str()),
                Err(_) => None,
            };
            assert_eq!(&actual, expected, "\nat case {i}: {:?}\n", case);
        }
    }
}
