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

use crate::Result;

#[inline(always)]
pub fn concat_op(left: &str, right: &str) -> Result<String> {
    Ok(left.chars().chain(right.chars()).collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_concat_op() {
        assert_eq!(concat_op("114", "514").unwrap(), "114514".to_owned())
    }
}
