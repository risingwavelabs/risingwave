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

use bytes::Buf;
use memcomparable::Result;
/// A structure that deserializes memcomparable bytes into Rust values.
pub struct Deserializer<B: Buf> {
    inner: memcomparable::Deserializer<B>,
}

impl<B: Buf> Deserializer<B> {
    /// Creates a deserializer from a buffer.
    pub fn new(input: B) -> Self {
        Deserializer {
            inner: memcomparable::Deserializer::new(input),
        }
    }

    /// Set whether data is serialized in reverse order.
    pub fn set_reverse(&mut self, reverse: bool) {
        self.inner.set_reverse(reverse);
    }

    /// Unwrap the inner buffer from the `Deserializer`.
    pub fn into_inner(self) -> B {
        self.inner.into_inner()
    }

    /// The inner is just a memcomparable deserializer. Removed in future.
    pub fn memcom_de(&mut self) -> &mut memcomparable::Deserializer<B> {
        &mut self.inner
    }

    /// Read u8 from Bytes input in decimal form (Do not include null tag). Used by value encoding
    /// ([`serialize_cell`]). TODO: It is a temporal solution For value encoding. Will moved to
    /// value encoding serializer in future.
    pub fn read_decimal_v2(&mut self) -> Result<Vec<u8>> {
        self.inner.read_decimal_v2()
    }
}
