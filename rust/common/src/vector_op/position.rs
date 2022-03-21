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
//
use crate::error::Result;

#[inline(always)]
/// Location of specified substring
///
/// Note: According to pgsql, position will return 0 rather -1 when substr is not in the target str
pub fn position(str: &str, sub_str: &str) -> Result<i32> {
    match str.find(sub_str) {
        Some(byte_idx) => Ok((str[..byte_idx].chars().count() + 1) as i32),
        None => Ok(0),
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_length() {
        let cases = [
            ("hello world", "world", Ok(7)),
            ("床前明月光", "月光", Ok(4)),
            ("床前明月光", "故乡", Ok(0)),
        ];

        for (str, sub_str, expected) in cases {
            println!("position is {}", position(str, sub_str).unwrap());
            assert_eq!(position(str, sub_str), expected)
        }
    }
}
