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

use std::iter;

use auto_enums::auto_enum;

use crate::buffer::{Bitmap, BitmapBuilder};

/// `Vis` is a visibility bitmap of rows. When all rows are visible, it is considered compact and
/// is represented by a single cardinality number rather than that many of ones.
#[derive(Clone, PartialEq, Debug)]
pub enum Vis {
    Bitmap(Bitmap),
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

impl From<&Vis> for Vis {
    fn from(vis: &Vis) -> Self {
        match vis {
            Vis::Bitmap(b) => b.clone().into(),
            Vis::Compact(c) => (*c).into(),
        }
    }
}

impl Vis {
    pub fn is_empty(&self) -> bool {
        match self {
            Vis::Bitmap(b) => b.is_empty(),
            Vis::Compact(c) => *c == 0,
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Vis::Bitmap(b) => b.len(),
            Vis::Compact(c) => *c,
        }
    }

    /// # Panics
    /// Panics if `idx > len`.
    pub fn is_set(&self, idx: usize) -> bool {
        match self {
            Vis::Bitmap(b) => b.is_set(idx),
            Vis::Compact(c) => {
                assert!(idx <= *c);
                true
            }
        }
    }

    #[auto_enum(Iterator)]
    pub fn iter(&self) -> impl Iterator<Item = bool> + '_ {
        match self {
            Vis::Bitmap(b) => b.iter(),
            Vis::Compact(c) => iter::repeat(true).take(*c),
        }
    }
}

impl<'a, 'b> std::ops::BitAnd<&'b Vis> for &'a Vis {
    type Output = Vis;

    fn bitand(self, rhs: &'b Vis) -> Self::Output {
        match (self, rhs) {
            (Vis::Bitmap(b1), Vis::Bitmap(b2)) => Vis::Bitmap(b1 & b2),
            (Vis::Bitmap(b1), Vis::Compact(c2)) => {
                assert_eq!(b1.len(), *c2);
                Vis::Bitmap(b1.clone())
            }
            (Vis::Compact(c1), Vis::Bitmap(b2)) => {
                assert_eq!(*c1, b2.len());
                Vis::Bitmap(b2.clone())
            }
            (Vis::Compact(c1), Vis::Compact(c2)) => {
                assert_eq!(*c1, *c2);
                Vis::Compact(*c1)
            }
        }
    }
}

impl<'a> std::ops::BitAnd<Vis> for &'a Vis {
    type Output = Vis;

    fn bitand(self, rhs: Vis) -> Self::Output {
        match (self, rhs) {
            (Vis::Bitmap(b1), Vis::Bitmap(b2)) => Vis::Bitmap(b1 & &b2),
            (Vis::Bitmap(b1), Vis::Compact(c2)) => {
                assert_eq!(b1.len(), c2);
                Vis::Bitmap(b1.clone())
            }
            (Vis::Compact(c1), Vis::Bitmap(b2)) => {
                assert_eq!(*c1, b2.len());
                Vis::Bitmap(b2)
            }
            (Vis::Compact(c1), Vis::Compact(c2)) => {
                assert_eq!(*c1, c2);
                Vis::Compact(*c1)
            }
        }
    }
}

impl<'a, 'b> std::ops::BitOr<&'b Vis> for &'a Vis {
    type Output = Vis;

    fn bitor(self, rhs: &'b Vis) -> Self::Output {
        match (self, rhs) {
            (Vis::Bitmap(b1), Vis::Bitmap(b2)) => Vis::Bitmap(b1 | b2),
            (Vis::Bitmap(b1), Vis::Compact(c2)) => {
                assert_eq!(b1.len(), *c2);
                Vis::Compact(*c2)
            }
            (Vis::Compact(c1), Vis::Bitmap(b2)) => {
                assert_eq!(*c1, b2.len());
                Vis::Compact(*c1)
            }
            (Vis::Compact(c1), Vis::Compact(c2)) => {
                assert_eq!(*c1, *c2);
                Vis::Compact(*c1)
            }
        }
    }
}

impl<'a> std::ops::BitOr<Vis> for &'a Vis {
    type Output = Vis;

    fn bitor(self, rhs: Vis) -> Self::Output {
        match (self, rhs) {
            (Vis::Bitmap(b1), Vis::Bitmap(b2)) => Vis::Bitmap(b1 | &b2),
            (Vis::Bitmap(b1), Vis::Compact(c2)) => {
                assert_eq!(b1.len(), c2);
                Vis::Compact(c2)
            }
            (Vis::Compact(c1), Vis::Bitmap(b2)) => {
                assert_eq!(*c1, b2.len());
                Vis::Compact(*c1)
            }
            (Vis::Compact(c1), Vis::Compact(c2)) => {
                assert_eq!(*c1, c2);
                Vis::Compact(*c1)
            }
        }
    }
}

impl<'a> std::ops::Not for &'a Vis {
    type Output = Vis;

    fn not(self) -> Self::Output {
        match self {
            Vis::Bitmap(b) => Vis::Bitmap(!b),
            Vis::Compact(c) => Vis::Bitmap(BitmapBuilder::zeroed(*c).finish()),
        }
    }
}
