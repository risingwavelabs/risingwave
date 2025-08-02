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

/// Extracts n'th bit from binary string.
#[function("get_bit(bytea, int8) -> int4")]
pub fn get_bit(bytes: &[u8], n: i64) -> Result<i32> {
    let max_sz = (bytes.len() * 8) as i64;
    if n < 0 || n >= max_sz {
        return Err(ExprError::InvalidParam {
            name: "get_bit",
            reason: format!("index {} out of valid range, 0..{}", n, max_sz - 1).into(),
        });
    }
    let index = n / 8;
    let byte = bytes[index as usize];
    Ok(((byte >> (n % 8)) & 1) as i32)
}

/// Sets n'th bit in binary string to newvalue.
#[function("set_bit(bytea, int8, int4) -> bytea")]
pub fn set_bit(bytes: &[u8], n: i64, value: i32) -> Result<Box<[u8]>> {
    let max_sz = (bytes.len() * 8) as i64;
    if n < 0 || n >= max_sz {
        return Err(ExprError::InvalidParam {
            name: "set_bit",
            reason: format!("index {} out of valid range, 0..{}", n, max_sz - 1).into(),
        });
    }
    let mut buf = bytes.to_vec();
    let index = (n / 8) as usize;
    let bit_pos = (n % 8) as u8;

    if value != 0 {
        buf[index] |= 1 << bit_pos;
    } else {
        buf[index] &= !(1 << bit_pos);
    }
    Ok(buf.iter().copied().collect())
}

/// Extracts n'th byte from binary string.
#[function("get_byte(bytea, int4) -> int4")]
pub fn get_byte(bytes: &[u8], n: i32) -> Result<i32> {
    let max_sz = bytes.len() as i32;
    if n < 0 || n >= max_sz {
        return Err(ExprError::InvalidParam {
            name: "get_byte",
            reason: format!("index {} out of valid range, 0..{}", n, max_sz - 1).into(),
        });
    }
    Ok(bytes[n as usize].into())
}

/// Sets n'th byte in binary string to newvalue.
#[function("set_byte(bytea, int4, int4) -> bytea")]
pub fn set_byte(bytes: &[u8], n: i32, value: i32) -> Result<Box<[u8]>> {
    let max_sz = bytes.len() as i32;
    if n < 0 || n >= max_sz {
        return Err(ExprError::InvalidParam {
            name: "set_byte",
            reason: format!("index {} out of valid range, 0..{}", n, max_sz - 1).into(),
        });
    }
    let mut buf = bytes.to_vec();
    buf[n as usize] = value as u8;
    Ok(buf.iter().copied().collect())
}

/// Returns the number of bits set in the binary string
#[function("bit_count(bytea) -> int8")]
pub fn bit_count(bytes: &[u8]) -> i64 {
    let mut ans = 0;
    for byte in bytes {
        ans += byte.count_ones();
    }
    ans.into()
}
