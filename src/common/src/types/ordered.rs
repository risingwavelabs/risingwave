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

//! `ScalarImpl` and `Datum` wrappers that implement `PartialOrd` and `Ord` with default order type.

use std::cmp::{Ord, Ordering};
use std::ops::Deref;

use crate::estimate_size::EstimateSize;
use crate::for_all_scalar_variants;
use crate::types::{Datum, DatumRef, ScalarImpl, ScalarRefImpl};
use crate::util::sort_util::{cmp_datum, partial_cmp_datum, OrderType};

macro_rules! gen_default_partial_cmp_scalar_ref_impl {
    ($( { $variant_name:ident, $suffix_name:ident, $scalar:ty, $scalar_ref:ty } ),*) => {
        pub fn default_partial_cmp_scalar_ref_impl(lhs: ScalarRefImpl<'_>, rhs: ScalarRefImpl<'_>) -> Option<Ordering> {
            match (lhs, rhs) {
                $((ScalarRefImpl::$variant_name(lhs_inner), ScalarRefImpl::$variant_name(ref rhs_inner)) => Some(lhs_inner.cmp(rhs_inner)),)*
                _ => None,
            }
        }
    };
}
for_all_scalar_variants!(gen_default_partial_cmp_scalar_ref_impl);

pub trait DefaultPartialOrd: PartialEq {
    fn default_partial_cmp(&self, other: &Self) -> Option<Ordering>;
}

/// Variant of [`Ord`] that compares with default order.
pub trait DefaultOrd: DefaultPartialOrd + Eq {
    fn default_cmp(&self, other: &Self) -> Ordering;
}

impl DefaultPartialOrd for ScalarImpl {
    fn default_partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.as_scalar_ref_impl()
            .default_partial_cmp(&other.as_scalar_ref_impl())
    }
}

impl DefaultOrd for ScalarImpl {
    fn default_cmp(&self, other: &Self) -> Ordering {
        self.as_scalar_ref_impl()
            .default_cmp(&other.as_scalar_ref_impl())
    }
}

impl DefaultPartialOrd for ScalarRefImpl<'_> {
    fn default_partial_cmp(&self, other: &Self) -> Option<Ordering> {
        default_partial_cmp_scalar_ref_impl(*self, *other)
    }
}

impl DefaultOrd for ScalarRefImpl<'_> {
    fn default_cmp(&self, other: &Self) -> Ordering {
        self.default_partial_cmp(other)
            .unwrap_or_else(|| panic!("cannot compare {self:?} with {other:?}"))
    }
}

impl DefaultPartialOrd for Datum {
    fn default_partial_cmp(&self, other: &Self) -> Option<Ordering> {
        partial_cmp_datum(self, other, OrderType::default())
    }
}

impl DefaultOrd for Datum {
    fn default_cmp(&self, other: &Self) -> Ordering {
        cmp_datum(self, other, OrderType::default())
    }
}

impl DefaultPartialOrd for DatumRef<'_> {
    fn default_partial_cmp(&self, other: &Self) -> Option<Ordering> {
        partial_cmp_datum(*self, *other, OrderType::default())
    }
}

impl DefaultOrd for DatumRef<'_> {
    fn default_cmp(&self, other: &Self) -> Ordering {
        cmp_datum(*self, *other, OrderType::default())
    }
}

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub struct DefaultOrdered<T: DefaultOrd> {
    inner: T,
}

impl<T: DefaultOrd + EstimateSize> EstimateSize for DefaultOrdered<T> {
    fn estimated_heap_size(&self) -> usize {
        self.inner.estimated_heap_size()
    }
}

impl<T: DefaultOrd> DefaultOrdered<T> {
    pub fn new(inner: T) -> Self {
        Self { inner }
    }

    pub fn into_inner(self) -> T {
        self.inner
    }

    pub fn as_inner(&self) -> &T {
        &self.inner
    }
}

impl<T: DefaultOrd> Deref for DefaultOrdered<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.as_inner()
    }
}

impl<T: DefaultOrd> From<T> for DefaultOrdered<T> {
    fn from(inner: T) -> Self {
        Self::new(inner)
    }
}

impl<T: DefaultOrd> PartialOrd for DefaultOrdered<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.inner.default_partial_cmp(other.as_inner())
    }
}

impl<T: DefaultOrd> Ord for DefaultOrdered<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.inner.default_cmp(other.as_inner())
    }
}

pub type OrdScalarImpl = DefaultOrdered<ScalarImpl>;
pub type OrdDatum = DefaultOrdered<Datum>;
