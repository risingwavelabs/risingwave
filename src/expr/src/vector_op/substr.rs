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

#[function("substr(varchar, int32) -> varchar")]
pub fn substr_start(s: &str, start: i32, writer: &mut dyn Write) -> Result<()> {
    let skip = start.saturating_sub(1).max(0) as usize;

    let substr = s.chars().skip(skip);
    for char in substr {
        writer.write_char(char).unwrap();
    }

    Ok(())
}

#[function("substr(varchar, int32, int32) -> varchar")]
pub fn substr_start_for(s: &str, start: i32, count: i32, writer: &mut dyn Write) -> Result<()> {
    if count < 0 {
        return Err(ExprError::InvalidParam {
            name: "length",
            reason: "negative substring length not allowed".to_string(),
        });
    }

    let skip = start.saturating_sub(1).max(0) as usize;
    let take = if start >= 1 {
        count as usize
    } else {
        count.saturating_add(start.saturating_sub(1)).max(0) as usize
    };

    let substr = s.chars().skip(skip).take(take);
    for char in substr {
        writer.write_char(char).unwrap();
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_substr() -> Result<()> {
        let s = "cxscgccdd";
        let us = "上海自来水来自海上";

        let cases = [
            (s, 4, None, "cgccdd"),
            (s, 4, Some(-2), "[unused result]"),
            (s, 4, Some(2), "cg"),
            (s, -1, Some(-5), "[unused result]"),
            (s, -1, Some(0), ""),
            (s, -1, Some(1), ""),
            (s, -1, Some(2), ""),
            (s, -1, Some(3), "c"),
            (s, -1, Some(5), "cxs"),
            // Unicode test
            (us, 1, Some(3), "上海自"),
            (us, 3, Some(3), "自来水"),
            (us, 6, Some(2), "来自"),
            (us, 6, Some(100), "来自海上"),
            (us, 6, None, "来自海上"),
            ("Mér", 1, Some(2), "Mé"),
        ];

        for (s, off, len, expected) in cases {
            let mut writer = String::new();
            match len {
                Some(len) => {
                    let result = substr_start_for(s, off, len, &mut writer);
                    if len < 0 {
                        assert!(result.is_err());
                        continue;
                    } else {
                        result?
                    }
                }
                None => substr_start(s, off, &mut writer)?,
            }
            assert_eq!(writer, expected);
        }
        Ok(())
    }
}
