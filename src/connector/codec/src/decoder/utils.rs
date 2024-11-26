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

use super::{bail_uncategorized, AccessResult};

pub fn extract_decimal(bytes: Vec<u8>) -> AccessResult<(u32, u32, u32)> {
    match bytes.len() {
        len @ 0..=4 => {
            let mut pad = vec![0; 4 - len];
            pad.extend_from_slice(&bytes);
            let lo = u32::from_be_bytes(pad.try_into().unwrap());
            Ok((lo, 0, 0))
        }
        len @ 5..=8 => {
            let zero_len = 8 - len;
            let mid_end = 4 - zero_len;

            let mut pad = vec![0; zero_len];
            pad.extend_from_slice(&bytes[..mid_end]);
            let mid = u32::from_be_bytes(pad.try_into().unwrap());

            let lo = u32::from_be_bytes(bytes[mid_end..].to_owned().try_into().unwrap());
            Ok((lo, mid, 0))
        }
        len @ 9..=12 => {
            let zero_len = 12 - len;
            let hi_end = 4 - zero_len;
            let mid_end = hi_end + 4;

            let mut pad = vec![0; zero_len];
            pad.extend_from_slice(&bytes[..hi_end]);
            let hi = u32::from_be_bytes(pad.try_into().unwrap());

            let mid = u32::from_be_bytes(bytes[hi_end..mid_end].to_owned().try_into().unwrap());

            let lo = u32::from_be_bytes(bytes[mid_end..].to_owned().try_into().unwrap());
            Ok((lo, mid, hi))
        }
        _ => bail_uncategorized!("invalid decimal bytes length {}", bytes.len()),
    }
}
