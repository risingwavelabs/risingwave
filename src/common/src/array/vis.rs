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

use std::mem;

use either::Either;
use itertools::repeat_n;

use crate::buffer::{Bitmap, BitmapBuilder, BitmapIter, BitmapOnesIter};
use crate::estimate_size::EstimateSize;

/// `Vis` is a visibility bitmap of rows.
#[derive(Clone, PartialEq, Debug)]
pub enum Vis {
    /// Non-compact variant.
    /// Certain rows are hidden using this bitmap.
    Bitmap(Bitmap),

    /// Compact variant which just stores cardinality of rows.
    /// This can be used when all rows are visible.
    Compact(usize), // equivalent to all ones of this size
}

impl From<Bitmap> for Vis {
    fn from(b: Bitmap) -> Self {
        Vis::Bitmap(b)
    }
}

impl From<usize> for Vis {
    fn from(c: usize) -> Self {
        Vis::Compact(c)
    }
}

impl From<VisMut> for Vis {
    fn from(v: VisMut) -> Self {
        match v.state {
            VisMutState::Bitmap(x) => Vis::Bitmap(x),
            VisMutState::Compact(x) => Vis::Compact(x),
            VisMutState::Builder(x) => Vis::Bitmap(x.finish()),
        }
    }
}

impl Vis {
    pub fn into_mut(self) -> VisMut {
        VisMut::from(self)
    }

    pub fn is_empty(&self) -> bool {
        self.as_ref().is_empty()
    }

    pub fn len(&self) -> usize {
        self.as_ref().len()
    }

    /// # Panics
    /// Panics if `idx > len`.
    pub fn is_set(&self, idx: usize) -> bool {
        self.as_ref().is_set(idx)
    }

    pub fn iter(&self) -> Iter<'_> {
        self.as_ref().iter()
    }

    pub fn iter_ones(&self) -> OnesIter<'_> {
        self.as_ref().iter_ones()
    }

    #[inline(always)]
    pub fn as_ref(&self) -> VisRef<'_> {
        match self {
            Vis::Bitmap(b) => VisRef::Bitmap(b),
            Vis::Compact(c) => VisRef::Compact(*c),
        }
    }

    /// Returns a bitmap of this `Vis`.
    pub fn to_bitmap(&self) -> Bitmap {
        match self {
            Vis::Bitmap(b) => b.clone(),
            Vis::Compact(c) => Bitmap::ones(*c),
        }
    }

    /// Consumes this `Vis` and returns the inner `Bitmap` if not compact.
    pub fn into_visibility(self) -> Option<Bitmap> {
        match self {
            Vis::Bitmap(b) => Some(b),
            Vis::Compact(_) => None,
        }
    }

    /// Returns a reference to the inner `Bitmap` if not compact.
    pub fn as_visibility(&self) -> Option<&Bitmap> {
        match self {
            Vis::Bitmap(b) => Some(b),
            Vis::Compact(_) => None,
        }
    }
}

impl EstimateSize for Vis {
    fn estimated_heap_size(&self) -> usize {
        match self {
            Vis::Bitmap(bitmap) => bitmap.estimated_heap_size(),
            Vis::Compact(_) => 0,
        }
    }
}

impl std::ops::BitAndAssign<&Bitmap> for Vis {
    fn bitand_assign(&mut self, rhs: &Bitmap) {
        match self {
            Vis::Bitmap(lhs) => lhs.bitand_assign(rhs),
            Vis::Compact(_) => *self = Vis::Bitmap(rhs.clone()),
        }
    }
}

impl std::ops::BitAndAssign<Bitmap> for Vis {
    fn bitand_assign(&mut self, rhs: Bitmap) {
        match self {
            Vis::Bitmap(lhs) => lhs.bitand_assign(&rhs),
            Vis::Compact(_) => *self = Vis::Bitmap(rhs),
        }
    }
}

impl std::ops::BitAnd<&Bitmap> for &Vis {
    type Output = Vis;

    fn bitand(self, rhs: &Bitmap) -> Self::Output {
        match self {
            Vis::Bitmap(lhs) => Vis::Bitmap(lhs.bitand(rhs)),
            Vis::Compact(_) => Vis::Bitmap(rhs.clone()),
        }
    }
}

impl<'a, 'b> std::ops::BitAnd<&'b Vis> for &'a Vis {
    type Output = Vis;

    fn bitand(self, rhs: &'b Vis) -> Self::Output {
        self.as_ref().bitand(rhs.as_ref())
    }
}

impl<'a> std::ops::BitAnd<Vis> for &'a Vis {
    type Output = Vis;

    fn bitand(self, rhs: Vis) -> Self::Output {
        self.as_ref().bitand(rhs)
    }
}

impl<'a, 'b> std::ops::BitOr<&'b Vis> for &'a Vis {
    type Output = Vis;

    fn bitor(self, rhs: &'b Vis) -> Self::Output {
        self.as_ref().bitor(rhs.as_ref())
    }
}

impl<'a> std::ops::Not for &'a Vis {
    type Output = Vis;

    fn not(self) -> Self::Output {
        self.as_ref().not()
    }
}

#[derive(Copy, Clone, PartialEq, Debug)]
pub enum VisRef<'a> {
    Bitmap(&'a Bitmap),
    Compact(usize), // equivalent to all ones of this size
}

pub type Iter<'a> = Either<BitmapIter<'a>, itertools::RepeatN<bool>>;
pub type OnesIter<'a> = Either<BitmapOnesIter<'a>, std::ops::Range<usize>>;

impl<'a> VisRef<'a> {
    pub fn is_empty(self) -> bool {
        match self {
            VisRef::Bitmap(b) => b.is_empty(),
            VisRef::Compact(c) => c == 0,
        }
    }

    pub fn len(self) -> usize {
        match self {
            VisRef::Bitmap(b) => b.len(),
            VisRef::Compact(c) => c,
        }
    }

    /// # Panics
    ///
    /// Panics if `idx > len`.
    pub fn is_set(self, idx: usize) -> bool {
        match self {
            VisRef::Bitmap(b) => b.is_set(idx),
            VisRef::Compact(c) => {
                assert!(idx <= c);
                true
            }
        }
    }

    pub fn iter(self) -> Iter<'a> {
        match self {
            VisRef::Bitmap(b) => Either::Left(b.iter()),
            VisRef::Compact(c) => Either::Right(repeat_n(true, c)),
        }
    }

    pub fn iter_ones(self) -> OnesIter<'a> {
        match self {
            VisRef::Bitmap(b) => Either::Left(b.iter_ones()),
            VisRef::Compact(c) => Either::Right(0..c),
        }
    }
}

impl<'a> From<&'a Bitmap> for VisRef<'a> {
    fn from(b: &'a Bitmap) -> Self {
        VisRef::Bitmap(b)
    }
}

impl<'a> From<usize> for VisRef<'a> {
    fn from(c: usize) -> Self {
        VisRef::Compact(c)
    }
}

impl<'a> From<&'a Vis> for VisRef<'a> {
    fn from(vis: &'a Vis) -> Self {
        vis.as_ref()
    }
}

impl<'a, 'b> std::ops::BitAnd<VisRef<'b>> for VisRef<'a> {
    type Output = Vis;

    fn bitand(self, rhs: VisRef<'b>) -> Self::Output {
        match (self, rhs) {
            (VisRef::Bitmap(b1), VisRef::Bitmap(b2)) => Vis::Bitmap(b1 & b2),
            (VisRef::Bitmap(b1), VisRef::Compact(c2)) => {
                assert_eq!(b1.len(), c2);
                Vis::Bitmap(b1.clone())
            }
            (VisRef::Compact(c1), VisRef::Bitmap(b2)) => {
                assert_eq!(c1, b2.len());
                Vis::Bitmap(b2.clone())
            }
            (VisRef::Compact(c1), VisRef::Compact(c2)) => {
                assert_eq!(c1, c2);
                Vis::Compact(c1)
            }
        }
    }
}

impl<'a> std::ops::BitAnd<Vis> for VisRef<'a> {
    type Output = Vis;

    fn bitand(self, rhs: Vis) -> Self::Output {
        match (self, rhs) {
            (VisRef::Bitmap(b1), Vis::Bitmap(b2)) => Vis::Bitmap(b1 & b2),
            (VisRef::Bitmap(b1), Vis::Compact(c2)) => {
                assert_eq!(b1.len(), c2);
                Vis::Bitmap(b1.clone())
            }
            (VisRef::Compact(c1), Vis::Bitmap(b2)) => {
                assert_eq!(c1, b2.len());
                Vis::Bitmap(b2)
            }
            (VisRef::Compact(c1), Vis::Compact(c2)) => {
                assert_eq!(c1, c2);
                Vis::Compact(c1)
            }
        }
    }
}

impl<'a, 'b> std::ops::BitOr<VisRef<'b>> for VisRef<'a> {
    type Output = Vis;

    fn bitor(self, rhs: VisRef<'b>) -> Self::Output {
        match (self, rhs) {
            (VisRef::Bitmap(b1), VisRef::Bitmap(b2)) => Vis::Bitmap(b1 | b2),
            (VisRef::Bitmap(b1), VisRef::Compact(c2)) => {
                assert_eq!(b1.len(), c2);
                Vis::Compact(c2)
            }
            (VisRef::Compact(c1), VisRef::Bitmap(b2)) => {
                assert_eq!(c1, b2.len());
                Vis::Compact(c1)
            }
            (VisRef::Compact(c1), VisRef::Compact(c2)) => {
                assert_eq!(c1, c2);
                Vis::Compact(c1)
            }
        }
    }
}

impl<'a> std::ops::BitOr<Vis> for VisRef<'a> {
    type Output = Vis;

    fn bitor(self, rhs: Vis) -> Self::Output {
        // Unlike the `bitand` implementation, we can forward by ref directly here, because this
        // will not introduce unnecessary clones.
        self.bitor(rhs.as_ref())
    }
}

impl<'a> std::ops::Not for VisRef<'a> {
    type Output = Vis;

    fn not(self) -> Self::Output {
        match self {
            VisRef::Bitmap(b) => Vis::Bitmap(!b),
            VisRef::Compact(c) => Vis::Bitmap(BitmapBuilder::zeroed(c).finish()),
        }
    }
}

/// A mutable wrapper for `Vis`. can only set the visibilities and can not change the size.
#[derive(Debug)]
pub struct VisMut {
    state: VisMutState,
}

#[derive(Debug)]
enum VisMutState {
    /// Non-compact variant.
    /// Certain rows are hidden using this bitmap.
    Bitmap(Bitmap),

    /// Compact variant which just stores cardinality of rows.
    /// This can be used when all rows are visible.
    Compact(usize), // equivalent to all ones of this size

    Builder(BitmapBuilder),
}

impl From<Vis> for VisMut {
    fn from(vis: Vis) -> Self {
        let state = match vis {
            Vis::Bitmap(x) => VisMutState::Bitmap(x),
            Vis::Compact(x) => VisMutState::Compact(x),
        };
        Self { state }
    }
}

impl VisMut {
    pub fn len(&self) -> usize {
        match &self.state {
            VisMutState::Bitmap(b) => b.len(),
            VisMutState::Compact(c) => *c,
            VisMutState::Builder(b) => b.len(),
        }
    }

    /// # Panics
    ///
    /// Panics if `idx >= len`.
    pub fn is_set(&self, idx: usize) -> bool {
        match &self.state {
            VisMutState::Bitmap(b) => b.is_set(idx),
            VisMutState::Compact(c) => {
                assert!(idx < *c);
                true
            }
            VisMutState::Builder(b) => b.is_set(idx),
        }
    }

    pub fn set(&mut self, n: usize, val: bool) {
        if let VisMutState::Builder(b) = &mut self.state {
            b.set(n, val);
        } else {
            let state = mem::replace(&mut self.state, VisMutState::Compact(0)); // intermediate state
            let mut builder = match state {
                VisMutState::Bitmap(b) => b.into(),
                VisMutState::Compact(c) => BitmapBuilder::filled(c),
                VisMutState::Builder(_) => unreachable!(),
            };
            builder.set(n, val);
            self.state = VisMutState::Builder(builder);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_vis_mut_from_compact() {
        let n: usize = 128;
        let vis = Vis::Compact(n);
        let mut vis = vis.into_mut();
        assert_eq!(vis.len(), n);

        for i in 0..n {
            assert!(vis.is_set(i), "{}", i);
        }
        vis.set(0, true);
        for i in 0..n {
            assert!(vis.is_set(i), "{}", i);
        }
        assert_eq!(vis.len(), n);

        let to_false = vec![1usize, 2, 14, 25, 17, 77, 62, 96];
        for i in &to_false {
            vis.set(*i, false);
        }
        assert_eq!(vis.len(), n);
        for i in 0..n {
            assert_eq!(vis.is_set(i), !to_false.contains(&i), "{}", i);
        }

        let vis: Vis = vis.into();
        assert_eq!(vis.len(), n);
        for i in 0..n {
            assert_eq!(vis.is_set(i), !to_false.contains(&i), "{}", i);
        }
        let count_ones = match &vis {
            Vis::Bitmap(b) => b.count_ones(),
            Vis::Compact(len) => *len,
        };
        assert_eq!(count_ones, n - to_false.len());
    }
    #[test]
    fn test_vis_mut_from_bitmap() {
        let zeros = 61usize;
        let ones = 62usize;
        let n: usize = ones + zeros;

        let mut builder = BitmapBuilder::default();
        builder.append_bitmap(&Bitmap::zeros(zeros));
        builder.append_bitmap(&Bitmap::ones(ones));

        let vis = Vis::Bitmap(builder.finish());
        assert_eq!(vis.len(), n);

        let mut vis = vis.into_mut();
        assert_eq!(vis.len(), n);
        for i in 0..n {
            assert_eq!(vis.is_set(i), i >= zeros, "{}", i);
        }

        vis.set(0, false);
        assert_eq!(vis.len(), n);
        for i in 0..n {
            assert_eq!(vis.is_set(i), i >= zeros, "{}", i);
        }

        let toggles = vec![1usize, 2, 14, 25, 17, 77, 62, 96];
        for i in &toggles {
            let i = *i;
            vis.set(i, i < zeros);
        }
        assert_eq!(vis.len(), n);
        for i in 0..zeros {
            assert_eq!(vis.is_set(i), toggles.contains(&i), "{}", i);
        }
        for i in zeros..n {
            assert_eq!(vis.is_set(i), !toggles.contains(&i), "{}", i);
        }

        let vis: Vis = vis.into();
        assert_eq!(vis.len(), n);
        for i in 0..zeros {
            assert_eq!(vis.is_set(i), toggles.contains(&i), "{}", i);
        }
        for i in zeros..n {
            assert_eq!(vis.is_set(i), !toggles.contains(&i), "{}", i);
        }
        let count_ones = match &vis {
            Vis::Bitmap(b) => b.count_ones(),
            Vis::Compact(len) => *len,
        };
        let mut expected_ones = ones;
        for i in &toggles {
            let i = *i;
            if i < zeros {
                expected_ones += 1;
            } else {
                expected_ones -= 1;
            }
        }
        assert_eq!(count_ones, expected_ones);
    }
}
