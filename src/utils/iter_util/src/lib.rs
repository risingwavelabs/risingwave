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

use std::collections::HashMap;

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

pub trait RwTupleItertools<A, B>: IntoIterator<Item = (A, B)> + Sized {
    // type Iter: Iterator<Item= Self::Item>;

    /// Gets the first element of the iterator.
    fn first<C: FromIterator<A>>(self) -> C {
        self.into_iter().map(|(a, _)| a).collect()
    }

    fn map_first<F, C, FA>(self, f: F) -> C
    where
        F: Fn(A) -> FA,
        C: FromIterator<(FA, B)>,
    {
        self.into_iter().map(|(a, b)| (f(a), b)).collect()
    }

    fn map_second<F, C, FB>(self, f: F) -> C
    where
        F: Fn(B) -> FB,
        C: FromIterator<(A, FB)>,
    {
        self.into_iter().map(|(a, b)| (a, f(b))).collect()
    }
}

impl<A, B, T> RwTupleItertools<A, B> for T where T: IntoIterator<Item = (A, B)> {}

// type IterFirst<I: IntoIterator> {}

#[test]
fn test() {
    let m: HashMap<i32, i32> = HashMap::from_iter([(1, 2), (3, 4)]);
    let m1: Vec<_> = (&m).first();
    println!("{:?}", m1);

    let m2: Vec<_> = (&m).map_first(|a| a * 2);
    println!("{:?}", m2);
}
