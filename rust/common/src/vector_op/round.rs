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

use crate::error::Result;
use crate::types::Decimal;

#[inline(always)]
pub fn round_digits<D: Into<i32>>(input: Decimal, digits: D) -> Result<Decimal> {
    let digits = digits.into();
    if digits < 0 {
        Ok(Decimal::zero())
    } else {
        Ok(input.round_dp(digits as u32))
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use crate::types::Decimal;
    use crate::vector_op::round::round_digits;

    fn do_test(input: &str, digits: i32, expected_output: &str) {
        let v = Decimal::from_str(input).unwrap();
        let rounded_value = round_digits(v, digits).unwrap();
        assert_eq!(expected_output, rounded_value.to_string().as_str());
    }

    #[test]
    fn test_round_digits() {
        do_test("21.666666666666666666666666667", 4, "21.6667");
        do_test("84818.33333333333333333333333", 4, "84818.3333");
        do_test("84818.15", 1, "84818.2");
        do_test("21.372736", -1, "0");
    }
}
