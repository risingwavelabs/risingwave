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

use std::fmt::Write;

use risingwave_common::array::{StringWriter, WrittenGuard};

use crate::Result;

#[inline(always)]
pub fn upper(s: &str, writer: StringWriter<'_>) -> Result<WrittenGuard> {
    let mut writer = writer.begin();
    for c in s.chars() {
        writer.write_char(c.to_ascii_uppercase()).unwrap();
    }
    Ok(writer.finish())
}

#[cfg(test)]
mod tests {
    use risingwave_common::array::{Array, ArrayBuilder, Utf8ArrayBuilder};

    use super::*;

    #[test]
    fn test_upper() -> Result<()> {
        let cases = [
            ("hello world", "HELLO WORLD"),
            ("hello RUST", "HELLO RUST"),
            ("3 apples", "3 APPLES"),
        ];

        for (s, expected) in cases {
            let mut builder = Utf8ArrayBuilder::new(1);
            let writer = builder.writer();
            let _guard = upper(s, writer)?;
            let array = builder.finish();
            let v = array.value_at(0).unwrap();
            assert_eq!(v, expected);
        }
        Ok(())
    }
}
