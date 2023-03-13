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

#[cfg(not(any(
    target_feature = "sse4.2",
    target_feature = "avx2",
    target_feature = "neon",
    target_feature = "simd128"
)))]
#[macro_export]
macro_rules! ensure_float {
    ($v:ident, $t:ty) => {
        $v.as_f64()
            .ok_or_else(|| anyhow!(concat!("expect ", stringify!($t), ", but found {}"), $v))?
    };
}

#[macro_export]
macro_rules! simd_json_ensure_float {
    ($v:ident, $t:ty) => {
        $v.cast_f64()
            .ok_or_else(|| anyhow!(concat!("expect ", stringify!($t), ", but found {}"), $v))?
    };
}

#[macro_export]
macro_rules! ensure_i16 {
    ($v:ident, $t:ty) => {
        $v.as_i16()
            .ok_or_else(|| anyhow!(concat!("expect ", stringify!($t), ", but found {}"), $v))?
    };
}

#[macro_export]
macro_rules! ensure_i32 {
    ($v:ident, $t:ty) => {
        $v.as_i32()
            .ok_or_else(|| anyhow!(concat!("expect ", stringify!($t), ", but found {}"), $v))?
    };
}

#[macro_export]
macro_rules! ensure_i64 {
    ($v:ident, $t:ty) => {
        $v.as_i64()
            .ok_or_else(|| anyhow!(concat!("expect ", stringify!($t), ", but found {}"), $v))?
    };
}

#[macro_export]
macro_rules! ensure_str {
    ($v:ident, $t:literal) => {
        $v.as_str()
            .ok_or_else(|| anyhow!(concat!("expect ", $t, ", but found {}"), $v))?
    };
}

#[macro_export]
macro_rules! ensure_rust_type {
    ($v:ident, $t:ty) => {
        $crate::ensure_str!($v, "string")
            .parse::<$t>()
            .map_err(|_| anyhow!("failed parse {} from {}", stringify!($t), $v))?
    };
}
