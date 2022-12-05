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

use num_traits::Zero;

/// A more general version of [`num_traits::CheckedAdd`] that allows `Rhs` and `Output` to be
/// different.
///
/// Its signature follows [`std::ops::Add`] to take `self` and `Rhs` rather than references used in
/// [`num_traits::CheckedAdd`]. If we need to implement ops on references, it can be `impl
/// CheckedAdd<&Bar> for &Foo`.
pub trait CheckedAdd<Rhs = Self> {
    type Output;
    fn checked_add(self, rhs: Rhs) -> Option<Self::Output>;
}

/// Types already impl [`num_traits::CheckedAdd`] automatically impl this extended trait. Note that
/// this only covers `T + T` but not `T + &T`, `&T + T` or `&T + &T`, which is used less frequently
/// for `Copy` types.
impl<T: num_traits::CheckedAdd> CheckedAdd for T {
    type Output = T;

    fn checked_add(self, rhs: T) -> Option<Self> {
        num_traits::CheckedAdd::checked_add(&self, &rhs)
    }
}

/// A simplified version of [`num_traits::Signed`].
pub trait IsNegative: Zero {
    fn is_negative(&self) -> bool;
}

impl<T: num_traits::Signed> IsNegative for T {
    fn is_negative(&self) -> bool {
        num_traits::Signed::is_negative(self)
    }
}
