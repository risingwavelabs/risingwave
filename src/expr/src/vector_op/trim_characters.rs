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

use itertools::Itertools;
use risingwave_common::array::{BytesGuard, BytesWriter};

use crate::Result;

macro_rules! gen_trim {
    ($( { $func_name:ident, $method:ident }),*) => {
        $(#[inline(always)]
        pub fn $func_name(s: &str, characters: &str, writer: BytesWriter) -> Result<BytesGuard> {
            let slices = characters.chars().collect_vec();
            // We remark that feeding a &str and a slice of chars into trim_left/right_matches
            // means different, one is matching with the entire string and the other one is matching
            // with any char in the slice.
            writer
                .write_ref(s.$method(&slices[..]))
                .map_err(Into::into)
        })*
    }
}

macro_rules! for_three_variants {
    ($macro:ident) => {
        $macro! {
            { trim_characters, trim_matches },
            { ltrim_characters, trim_start_matches },
            { rtrim_characters, trim_end_matches }
        }
    };
}

for_three_variants! { gen_trim }

#[cfg(test)]
mod tests {
    use risingwave_common::array::{Array, ArrayBuilder, Utf8ArrayBuilder};

    use super::*;

    #[test]
    fn test_trim_characters() {
        let cases = [
            ("Hello world", "Hdl", "ello wor"),
            ("abcde", "aae", "bcd"),
            ("zxy", "yxz", ""),
        ];

        for (s, characters, expected) in cases {
            let builder = Utf8ArrayBuilder::new(1);
            let writer = builder.writer();
            let guard = trim_characters(s, characters, writer).unwrap();
            let array = guard.into_inner().finish();
            let v = array.value_at(0).unwrap();
            assert_eq!(v, expected);
        }
    }

    #[test]
    fn test_ltrim_characters() {
        let cases = [
            ("Hello world", "Hdl", "ello world"),
            ("abcde", "aae", "bcde"),
            ("zxy", "yxz", ""),
        ];

        for (s, characters, expected) in cases {
            let builder = Utf8ArrayBuilder::new(1);
            let writer = builder.writer();
            let guard = ltrim_characters(s, characters, writer).unwrap();
            let array = guard.into_inner().finish();
            let v = array.value_at(0).unwrap();
            assert_eq!(v, expected);
        }
    }

    #[test]
    fn test_rtrim_characters() {
        let cases = [
            ("Hello world", "Hdl", "Hello wor"),
            ("abcde", "aae", "abcd"),
            ("zxy", "yxz", ""),
        ];

        for (s, characters, expected) in cases {
            let builder = Utf8ArrayBuilder::new(1);
            let writer = builder.writer();
            let guard = rtrim_characters(s, characters, writer).unwrap();
            let array = guard.into_inner().finish();
            let v = array.value_at(0).unwrap();
            assert_eq!(v, expected);
        }
    }
}
