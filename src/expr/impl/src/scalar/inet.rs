// Copyright 2024 RisingWave Labs
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

use risingwave_expr::{function, ExprError, Result};

/// Given the dotted-quad representation of an IPv4 network address as a string,
/// returns an integer that represents the numeric value of the address
/// in network byte order (big endian). The returning value is a BIGINT (8-byte integer)
/// because PG doesn't support unsigned 32-bit integer.
///
/// Short-form IP addresses (such as '127.1' as a representation of '127.0.0.1')
/// are NOT supported.
///
/// This function is ported from MySQL.
/// Ref: <https://dev.mysql.com/doc/refman/8.3/en/miscellaneous-functions.html#function_inet-aton>.
///
/// # Example
///
/// ```slt
/// query I
/// select inet_aton('10.0.5.9');
/// ----
/// 167773449
/// ```
#[function("inet_aton(varchar) -> int8")]
pub fn inet_aton(str: &str) -> Result<i64> {
    let mut parts = str.split('.');
    let mut result = 0;
    for _ in 0..4 {
        let part = parts.next().ok_or(ExprError::InvalidParam {
            name: "str",
            reason: format!("Invalid IP address: {}", &str).into(),
        })?;
        let part = part.parse::<u8>().map_err(|_| ExprError::InvalidParam {
            name: "str",
            reason: format!("Invalid IP address: {}", &str).into(),
        })?;
        result = (result << 8) | part as i64;
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;

    use super::*;

    #[test]
    fn test_inet_aton() {
        assert_eq!(inet_aton("10.0.5.9").unwrap(), 167773449);
        assert_eq!(inet_aton("203.117.31.34").unwrap(), 3413450530);

        if let ExprError::InvalidParam { name, reason } = inet_aton("127.1").unwrap_err() {
            assert_eq!(name, "str");
            assert_eq!(reason, "Invalid IP address: 127.1".into());
        } else {
            panic!("Expected InvalidParam error");
        }

        assert_matches!(inet_aton("127.0.1"), Err(ExprError::InvalidParam { .. }));
        assert_matches!(inet_aton("1.0.0.256"), Err(ExprError::InvalidParam { .. }));
        assert_matches!(inet_aton("1.0.0.-1"), Err(ExprError::InvalidParam { .. }));
    }
}
