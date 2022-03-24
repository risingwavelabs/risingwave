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

use bytes::BufMut;

/// A structure for serializing Rust values into a memcomparable bytes.
pub struct Serializer<B: BufMut> {
    inner: memcomparable::Serializer<B>,
}

impl<B: BufMut> Serializer<B> {
    /// Create a new `Serializer`.
    pub fn new(buffer: B) -> Self {
        Serializer {
            inner: memcomparable::Serializer::new(buffer),
        }
    }

    /// Unwrap the inner buffer from the `Serializer`.
    pub fn into_inner(self) -> B {
        self.inner.into_inner()
    }

    /// Set whether data is serialized in reverse order.
    pub fn set_reverse(&mut self, reverse: bool) {
        self.inner.set_reverse(reverse)
    }

    /// The inner is just a memcomparable serializer. Removed in future.
    pub fn memcom_ser(&mut self) -> &mut memcomparable::Serializer<B> {
        &mut self.inner
    }
}
