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

use crate::{bail, ExprError, Result};

#[inline(always)]
pub fn substr_start(s: &str, start: i32, writer: &mut dyn Write) -> Result<()> {
    let start = (start
        .checked_sub(1)
        .ok_or(ExprError::NumericOutOfRange)?
        .max(0) as usize)
        .min(s.len());
    writer.write_str(&s[start..]).unwrap();
    Ok(())
}

#[inline(always)]
pub fn substr_for(s: &str, count: i32, writer: &mut dyn Write) -> Result<()> {
    let end = min(count as usize, s.len());
    writer.write_str(&s[..end]).unwrap();
    Ok(())
}

#[inline(always)]
pub fn substr_start_for(s: &str, start: i32, count: i32, writer: &mut dyn Write) -> Result<()> {
    if count < 0 {
        bail!("length in substr should be non-negative: {}", count);
    }
    let start = start.checked_sub(1).ok_or(ExprError::NumericOutOfRange)?;
    let begin = max(start, 0) as usize;
    let end = (start
        .checked_add(count)
        .ok_or(ExprError::NumericOutOfRange)? as usize)
        .min(s.len());
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
            (s.to_owned(), Some(4), None, "cgccdd"),
            (s.to_owned(), None, Some(3), "cxs"),
            (s.to_owned(), Some(4), Some(-2), "[unused result]"),
            (s.to_owned(), Some(4), Some(2), "cg"),
            (s.to_owned(), Some(-1), Some(-5), "[unused result]"),
            (s.to_owned(), Some(-1), Some(5), "cxs"),
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
