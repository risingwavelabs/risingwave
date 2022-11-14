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

use md5 as lib_md5;
use risingwave_common::array::{BytesGuard, BytesWriter};

use crate::Result;

#[inline(always)]
pub fn md5(s: &str, writer: BytesWriter) -> Result<BytesGuard> {
    writer
        .write_ref(&format!("{:x}", lib_md5::compute(s)))
        .map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use risingwave_common::array::{Array, ArrayBuilder, Utf8ArrayBuilder};

    use super::*;

    #[test]
    fn test_md5() -> Result<()> {
        let cases = [
            ("hello world", "5eb63bbbe01eeed093cb22bb8f5acdc3"),
            ("hello RUST", "917b821a0a5f23ab0cfdb36056d2eb9d"),
            (
                "abcdefghijklmnopqrstuvwxyz",
                "c3fcd3d76192e4007dfb496cca67e13b",
            ),
        ];

        for (s, expected) in cases {
            let builder = Utf8ArrayBuilder::new(1);
            let writer = builder.writer();
            let guard = md5(s, writer)?;
            let array = guard.into_inner().finish();
            let v = array.value_at(0).unwrap();
            assert_eq!(v, expected);
        }
        Ok(())
    }
}
