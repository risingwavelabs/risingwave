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

#[function("repeat(varchar, int32) -> varchar")]
pub fn repeat(s: &str, count: i32, writer: &mut dyn Write) {
    for _ in 0..count {
        writer.write_str(s).unwrap();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_repeat() {
        let cases = vec![
            ("hello, world", 1, "hello, world"),
            ("114514", 3, "114514114514114514"),
            ("ssss", 0, ""),
            ("ssss", -114514, ""),
        ];

        for (s, count, expected) in cases {
            let mut writer = String::new();
            repeat(s, count, &mut writer);
            assert_eq!(writer, expected);
        }
    }
}
