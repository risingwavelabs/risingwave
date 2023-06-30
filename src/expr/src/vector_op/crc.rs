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

use crc32fast;
use risingwave_expr_macro::function;

use crate::Result;

#[function("crc32(varchar) -> int32")]
pub fn crc32(str: &str) -> Result<i32> {
    Ok(crc32fast::hash(str.as_ref()) as i32)
}

#[cfg(test)]
mod tests {
    use super::crc32;

    #[test]
    fn test_crc32() {
        assert_eq!(crc32("foo bar baz").unwrap(), 4066565729);
    }
}
