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

use crate::Result;

/// Note: the behavior of `ltrim` in `PostgreSQL` and `trim_start` (or `trim_left`) in Rust
/// are actually different when the string is in right-to-left languages like Arabic or Hebrew.
/// Since we would like to simplify the implementation, currently we omit this case.
#[function("ltrim(varchar) -> varchar")]
pub fn ltrim(s: &str, writer: &mut dyn Write) -> Result<()> {
    writer.write_str(s.trim_start()).unwrap();
    Ok(())
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_ltrim() -> Result<()> {
        let cases = [
            (" \tHello\tworld\t", "Hello\tworld\t"),
            (" \t空I ❤️ databases空 ", "空I ❤️ databases空 "),
        ];

        for (s, expected) in cases {
            let mut writer = String::new();
            ltrim(s, &mut writer)?;
            assert_eq!(writer, expected);
        }
        Ok(())
    }
}
