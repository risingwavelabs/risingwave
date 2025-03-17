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

use risingwave_expr::{ExprError, Result, function};

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

/// Given a numeric IPv4 network address in network byte order (big endian),
/// returns the dotted-quad string representation of the address as a string.
///
/// This function is ported from MySQL.
/// Ref: <https://dev.mysql.com/doc/refman/8.3/en/miscellaneous-functions.html#function_inet-ntoa>.
///
/// # Example
///
/// ```slt
/// query T
/// select inet_ntoa(167773449);
/// ----
/// 10.0.5.9
/// ```
#[function("inet_ntoa(int8) -> varchar")]
pub fn inet_ntoa(mut num: i64) -> Result<Box<str>> {
    if (num > u32::MAX as i64) || (num < 0) {
        return Err(ExprError::InvalidParam {
            name: "num",
            reason: format!("Invalid IP number: {}", num).into(),
        });
    }
    let mut parts = [0u8, 0, 0, 0];
    for i in (0..4).rev() {
        parts[i] = (num & 0xFF) as u8;
        num >>= 8;
    }
    let str = parts
        .iter()
        .map(|&x| x.to_string())
        .collect::<Vec<_>>()
        .join(".");
    Ok(str.into_boxed_str())
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

    #[test]
    fn test_inet_ntoa() {
        assert_eq!(inet_ntoa(167773449).unwrap(), "10.0.5.9".into());
        assert_eq!(inet_ntoa(3413450530).unwrap(), "203.117.31.34".into());
        assert_eq!(inet_ntoa(0).unwrap(), "0.0.0.0".into());
        assert_eq!(
            inet_ntoa(u32::MAX as i64).unwrap(),
            "255.255.255.255".into()
        );

        if let ExprError::InvalidParam { name, reason } = inet_ntoa(-1).unwrap_err() {
            assert_eq!(name, "num");
            assert_eq!(reason, "Invalid IP number: -1".into());
        } else {
            panic!("Expected InvalidParam error");
        }

        assert_matches!(
            inet_ntoa(u32::MAX as i64 + 1),
            Err(ExprError::InvalidParam { .. })
        );
    }
}
