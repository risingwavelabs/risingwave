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

#[function("concat_op(varchar, varchar) -> varchar")]
pub fn concat_op(left: &str, right: &str, writer: &mut dyn Write) -> Result<()> {
    writer.write_str(left).unwrap();
    writer.write_str(right).unwrap();
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_concat_op() {
        let mut s = String::new();
        concat_op("114", "514", &mut s).unwrap();
        assert_eq!(s, "114514")
    }
}
