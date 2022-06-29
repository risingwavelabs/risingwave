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

macro_rules! for_all_unsigned_type {
    ($macro:ident $(, $x:tt)*) => {
        $macro! {
            {u8, u8},
            {u16,u16},
            {u32, u32},
            {u64, u64},
            {usize, usize},
        }
    };
}

macro_rules! impl_utils {
    ($({$mod:ident, $t:ty},)*) => {
        $(
            pub mod $mod{
                #[inline(always)]
                pub fn is_pow2(v: $t) -> bool {
                    v & (v - 1) == 0
                }

                #[inline(always)]
                pub fn assert_pow2(v: $t) {
                    assert_eq!(v & (v - 1), 0, "v: {}", v);
                }

                #[inline(always)]
                pub fn debug_assert_pow2(v: $t) {
                    debug_assert_eq!(v & (v - 1), 0, "v: {}", v);
                }

                #[inline(always)]
                pub fn is_aligned(align: $t, v: $t) -> bool {
                    debug_assert_pow2(align);
                    v & (align - 1) == 0
                }

                #[inline(always)]
                pub fn assert_aligned(align: $t, v: $t) {
                    debug_assert_pow2(align);
                    assert_eq!(v & (align - 1), 0, "align: {}, v: {}", align, v);
                }

                #[inline(always)]
                pub fn debug_assert_aligned(align: $t, v: $t) {
                    debug_assert_pow2(align);
                    debug_assert_eq!(v & (align - 1), 0, "align: {}, v: {}", align, v);
                }

                #[inline(always)]
                pub fn align_up(align: $t, v: $t) -> $t {
                    debug_assert_pow2(align);
                    (v + align - 1) & !(align - 1)
                }

                #[inline(always)]
                pub fn align_down(align: $t, v: $t) -> $t {
                    debug_assert_pow2(align);
                    v & !(align - 1)
                }
            }

        )*
    };
}

for_all_unsigned_type! { impl_utils }
