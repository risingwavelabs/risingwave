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
pub fn repeat(s: &str, count: i32, writer: BytesWriter) -> Result<BytesGuard> {
    let mut writer = writer.begin();
    for _ in 0..count {
        writer.write_ref(s)?;
    }
    writer.finish().map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use risingwave_common::array::{Array, ArrayBuilder, Utf8ArrayBuilder};

    use super::*;

    #[test]
    fn test_repeat() -> Result<()> {
        let cases = vec![
            ("hello, world", 1, "hello, world"),
            ("114514", 3, "114514114514114514"),
            ("ssss", 0, ""),
            ("ssss", -114514, ""),
        ];

        for (s, count, expected) in cases {
            let builder = Utf8ArrayBuilder::new(1);
            let writer = builder.writer();
            let guard = repeat(s, count, writer).unwrap();
            let array = guard.into_inner().finish();
            let v = array.value_at(0).unwrap();
            assert_eq!(v, expected);
        }
        Ok(())
    }
}
