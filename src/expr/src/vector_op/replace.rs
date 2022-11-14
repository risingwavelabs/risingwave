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

use risingwave_common::array::{BytesGuard, BytesWriter};

use crate::Result;

#[inline(always)]
pub fn replace(s: &str, from_str: &str, to_str: &str, writer: BytesWriter) -> Result<BytesGuard> {
    if from_str.is_empty() {
        return writer.write_ref(s).map_err(Into::into);
    }
    let mut last = 0;
    let mut writer = writer.begin();
    while let Some(mut start) = s[last..].find(from_str) {
        start += last;
        writer.write_ref(&s[last..start])?;
        writer.write_ref(to_str)?;
        last = start + from_str.len();
    }
    writer.write_ref(&s[last..])?;
    writer.finish().map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use risingwave_common::array::{Array, ArrayBuilder, Utf8ArrayBuilder};

    use super::*;

    #[test]
    fn test_replace() {
        let cases = vec![
            ("hello, word", "我的", "world", "hello, word"),
            ("hello, word", "", "world", "hello, word"),
            ("hello, word", "word", "world", "hello, world"),
            ("hello, world", "world", "", "hello, "),
            ("你是❤️，是暖，是希望", "是", "非", "你非❤️，非暖，非希望"),
            ("👴笑了", "👴", "爷爷", "爷爷笑了"),
        ];

        for (s, from_str, to_str, expected) in cases {
            let builder = Utf8ArrayBuilder::new(1);
            let writer = builder.writer();
            let guard = replace(s, from_str, to_str, writer).unwrap();
            let array = guard.into_inner().finish();
            let v = array.value_at(0).unwrap();
            assert_eq!(v, expected);
        }
    }
}
