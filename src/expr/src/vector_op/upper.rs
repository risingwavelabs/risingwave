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

#[function("upper(varchar) -> varchar")]
pub fn upper(s: &str, writer: &mut dyn Write) -> Result<()> {
    for c in s.chars() {
        writer.write_char(c.to_ascii_uppercase()).unwrap();
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_upper() -> Result<()> {
        let cases = [
            ("hello world", "HELLO WORLD"),
            ("hello RUST", "HELLO RUST"),
            ("3 apples", "3 APPLES"),
        ];

        for (s, expected) in cases {
            let mut writer = String::new();
            upper(s, &mut writer)?;
            assert_eq!(writer, expected);
        }
        Ok(())
    }
}
