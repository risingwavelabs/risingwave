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

use std::hash::{BuildHasher, Hasher};

pub fn finalize_hashers<H: Hasher>(hashers: &mut [H]) -> Vec<u64> {
    return hashers
        .iter()
        .map(|hasher| hasher.finish())
        .collect::<Vec<u64>>();
}

#[derive(Clone, Copy)]
pub struct Crc32FastBuilder;

impl BuildHasher for Crc32FastBuilder {
    type Hasher = crc32fast::Hasher;

    fn build_hasher(&self) -> Self::Hasher {
        crc32fast::Hasher::new()
    }
}

#[derive(Clone, Copy)]
pub struct XxHash64Builder;

impl BuildHasher for XxHash64Builder {
    type Hasher = twox_hash::XxHash64;

    fn build_hasher(&self) -> Self::Hasher {
        twox_hash::XxHash64::with_seed(0)
    }
}
