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

use std::cmp::{max, min};
use std::fmt::Write;

use risingwave_expr_macro::function;

use crate::{bail, Result};

#[function("substr(varchar, int32) -> varchar")]
pub fn substr_start(s: &str, start: i32, writer: &mut dyn Write) -> Result<()> {
    let start = (start.saturating_sub(1).max(0) as usize).min(s.len());
    writer.write_str(&s[start..]).unwrap();
    Ok(())
}

// #[function("substr(varchar, 0, int32) -> varchar")]
pub fn substr_for(s: &str, count: i32, writer: &mut dyn Write) -> Result<()> {
    let end = min(count as usize, s.len());
    writer.write_str(&s[..end]).unwrap();
    Ok(())
}

#[function("substr(varchar, int32, int32) -> varchar")]
pub fn substr_start_for(s: &str, start: i32, count: i32, writer: &mut dyn Write) -> Result<()> {
    if count < 0 {
        bail!("length in substr should be non-negative: {}", count);
    }
    let start = start.saturating_sub(1);
    // NOTE: we use `s.len()` here as an upper bound.
    // This is so it will return an empty slice if it exceeds
    // the length of `s`.
    // 0 <= begin <= s.len()
    let begin = min(max(start, 0) as usize, s.len());
    let end = (start.saturating_add(count).max(0) as usize).min(s.len());
    writer.write_str(&s[begin..end]).unwrap();
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_substr() -> Result<()> {
        let s = "cxscgccdd";

        let cases = [
            (s, Some(4), None, "cgccdd"),
            (s, None, Some(3), "cxs"),
            (s, Some(4), Some(-2), "[unused result]"),
            (s, Some(4), Some(2), "cg"),
            (s, Some(-1), Some(-5), "[unused result]"),
            (s, Some(-1), Some(5), "cxs"),
        ];

        for (s, off, len, expected) in cases {
            let mut writer = String::new();
            match (off, len) {
                (Some(off), Some(len)) => {
                    let result = substr_start_for(&s, off, len, &mut writer);
                    if len < 0 {
                        assert!(result.is_err());
                        continue;
                    } else {
                        result?
                    }
                }
                (Some(off), None) => substr_start(&s, off, &mut writer)?,
                (None, Some(len)) => substr_for(&s, len, &mut writer)?,
                _ => unreachable!(),
            }
            assert_eq!(writer, expected);
        }
        Ok(())
    }
}
