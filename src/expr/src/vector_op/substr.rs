// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::cmp::{max, min};

use risingwave_common::array::{BytesGuard, BytesWriter};

use crate::{bail, Result};

#[inline(always)]
pub fn substr_start(s: &str, start: i32, writer: BytesWriter) -> Result<BytesGuard> {
    let start = min(max(start - 1, 0) as usize, s.len());
    writer.write_ref(&s[start..]).map_err(Into::into)
}

#[inline(always)]
pub fn substr_for(s: &str, count: i32, writer: BytesWriter) -> Result<BytesGuard> {
    let end = min(count as usize, s.len());
    writer.write_ref(&s[..end]).map_err(Into::into)
}

#[inline(always)]
pub fn substr_start_for(
    s: &str,
    start: i32,
    count: i32,
    writer: BytesWriter,
) -> Result<BytesGuard> {
    if count < 0 {
        bail!("length in substr should be non-negative: {}", count);
    }
    let begin = max(start - 1, 0) as usize;
    let end = min(max(start - 1 + count, 0) as usize, s.len());
    writer.write_ref(&s[begin..end]).map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use risingwave_common::array::{Array, ArrayBuilder, Utf8ArrayBuilder};

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
            let builder = Utf8ArrayBuilder::new(1);
            let writer = builder.writer();
            let guard = match (off, len) {
                (Some(off), Some(len)) => {
                    let result = substr_start_for(&s, off, len, writer);
                    if len < 0 {
                        assert!(result.is_err());
                        continue;
                    } else {
                        result?
                    }
                }
                (Some(off), None) => substr_start(&s, off, writer)?,
                (None, Some(len)) => substr_for(&s, len, writer)?,
                _ => unreachable!(),
            };
            let array = guard.into_inner().finish();
            let v = array.value_at(0).unwrap();
            assert_eq!(v, expected);
        }
        Ok(())
    }
}
