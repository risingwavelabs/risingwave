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

use auto_enums::auto_enum;
use itertools::repeat_n;

use crate::buffer::{Bitmap, BitmapBuilder};
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

impl Vis {
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

    pub fn iter(&self) -> impl ExactSizeIterator<Item = bool> + '_ {
        self.as_ref().iter()
    }

    pub fn iter_ones(&self) -> impl Iterator<Item = usize> + '_ {
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

    #[auto_enum(ExactSizeIterator)]
    pub fn iter(self) -> impl ExactSizeIterator<Item = bool> + 'a {
        match self {
            VisRef::Bitmap(b) => b.iter(),
            VisRef::Compact(c) => repeat_n(true, c),
        }
    }

    #[auto_enum(Iterator)]
    pub fn iter_ones(self) -> impl Iterator<Item = usize> + 'a {
        match self {
            VisRef::Bitmap(b) => b.iter_ones(),
            VisRef::Compact(c) => 0..c,
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
