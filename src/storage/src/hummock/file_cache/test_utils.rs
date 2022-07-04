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

use bytes::{Buf, BufMut};

use super::coding::CacheKey;

#[derive(Clone, Hash, Debug, PartialEq, Eq)]
pub struct TestCacheKey(pub u64);

impl CacheKey for TestCacheKey {
    fn encoded_len() -> usize {
        8
    }

    fn encode(&self, mut buf: &mut [u8]) {
        buf.put_u64(self.0);
    }

    fn decode(mut buf: &[u8]) -> Self {
        Self(buf.get_u64())
    }
}

pub fn key(v: u64) -> TestCacheKey {
    TestCacheKey(v)
}
