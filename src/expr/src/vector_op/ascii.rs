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

use risingwave_expr_macro::scalar;

use crate::Result;

#[scalar("(varchar) -> int32")]
pub fn ascii(s: &str) -> Result<i32> {
    Ok(s.as_bytes().first().map(|x| *x as i32).unwrap_or(0))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ascii() {
        let cases = [("hello", 104), ("你好", 228), ("", 0)];
        for (s, expected) in cases {
            assert_eq!(ascii(s).unwrap(), expected)
        }
    }
}
