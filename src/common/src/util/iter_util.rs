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

pub trait ZipEqFast<B: IntoIterator>: ExactSizeIterator + Sized
where
    B::IntoIter: ExactSizeIterator,
{
    /// A specialized version of `zip_eq` for [`ExactSizeIterator`].
    ///
    /// It's a separate trait because Rust doesn't support specialization yet.
    /// See [tracking issue for specialization (RFC 1210)](https://github.com/rust-lang/rust/issues/31844).
    #[expect(clippy::disallowed_methods)]
    fn zip_eq_fast(self, other: B) -> impl ExactSizeIterator<Item = (Self::Item, B::Item)> {
        let other = other.into_iter();
        assert_eq!(self.len(), other.len());
        self.zip(other)
    }
}

impl<A: ExactSizeIterator, B: IntoIterator> ZipEqFast<B> for A where B::IntoIter: ExactSizeIterator {}

pub trait ZipEqDebug<B: IntoIterator>: itertools::Itertools + Sized {
    /// Use `zip_eq` when `debug_assertions` is enabled, otherwise use `zip`.
    ///
    /// It's because `zip_eq` has a very large overhead of checking each item in the iterators.
    #[expect(clippy::disallowed_methods)]
    fn zip_eq_debug(self, other: B) -> impl Iterator<Item = (Self::Item, B::Item)> {
        #[cfg(debug_assertions)]
        return self.zip_eq(other);
        #[cfg(not(debug_assertions))]
        return self.zip(other);
    }
}

impl<A: itertools::Itertools + Sized, B: IntoIterator> ZipEqDebug<B> for A {}

pub fn zip_eq_fast<A, B>(a: A, b: B) -> impl Iterator<Item = (A::Item, B::Item)>
where
    A: IntoIterator,
    B: IntoIterator,
    A::IntoIter: ExactSizeIterator,
    B::IntoIter: ExactSizeIterator,
{
    a.into_iter().zip_eq_fast(b)
}
