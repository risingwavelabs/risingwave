// Copyright 2025 RisingWave Labs
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

use risingwave_common::types::Int256;
use risingwave_expr::ExprError::Parse;
use risingwave_expr::{Result, function};
use thiserror_ext::AsReport;
const MAX_AVAILABLE_HEX_STR_LEN: usize = 66;

/// Returns the integer value of the hexadecimal string.
///
/// # Example
///
/// ```slt
/// query I
/// select hex_to_int256('0xdeadbeef');
/// ----
/// 3735928559
/// ```
#[function("hex_to_int256(varchar) -> int256")]
pub fn hex_to_int256(s: &str) -> Result<Int256> {
    Int256::from_str_hex(s).map_err(|e| {
        Parse(
            if s.len() <= MAX_AVAILABLE_HEX_STR_LEN {
                format!("failed to parse hex '{}', {}", s, e.as_report())
            } else {
                format!(
                    "failed to parse hex '{}...'(truncated, total {} bytes), {}",
                    &s[..MAX_AVAILABLE_HEX_STR_LEN],
                    s.len(),
                    e.as_report()
                )
            }
            .into(),
        )
    })
}

#[cfg(test)]
mod tests {
    use risingwave_common::types::Int256;
    use risingwave_expr::ExprError::Parse;

    use crate::scalar::int256::hex_to_int256;

    #[test]
    fn test_hex_to_int256() {
        assert_eq!(hex_to_int256("0x0").unwrap(), Int256::from(0));
        assert_eq!(hex_to_int256("0x0000").unwrap(), Int256::from(0));
        assert_eq!(hex_to_int256("0x1").unwrap(), Int256::from(1));

        assert_eq!(hex_to_int256("0xf").unwrap(), Int256::from(15));
        assert_eq!(hex_to_int256("0xff").unwrap(), Int256::from(255));

        assert_eq!(
            hex_to_int256("0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff")
                .unwrap(),
            Int256::from(-1)
        );
        assert_eq!(
            hex_to_int256("0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff01")
                .unwrap(),
            Int256::from(-255)
        );

        // int256 max
        assert_eq!(
            hex_to_int256("0x7fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff")
                .unwrap(),
            Int256::max_value(),
        );

        // int256 min
        assert_eq!(
            hex_to_int256("0x8000000000000000000000000000000000000000000000000000000000000000")
                .unwrap(),
            Int256::min_value(),
        );
    }

    #[test]
    fn test_failed() {
        let failed_result = hex_to_int256("0xggggggg");
        assert!(failed_result.is_err());
        assert!(matches!(failed_result.as_ref().err(), Some(Parse(_))));
    }
}
