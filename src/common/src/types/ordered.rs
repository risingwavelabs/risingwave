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

use paste::paste;
use risingwave_common_proc_macro::EstimateSize;

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

/// Variant of [`PartialOrd`] that compares with default order.
pub trait DefaultPartialOrd {
    fn default_partial_cmp(&self, other: &Self) -> Option<Ordering>;
}

/// Variant of [`Ord`] that compares with default order.
pub trait DefaultOrd: DefaultPartialOrd {
    fn default_cmp(&self, other: &Self) -> Ordering;
}

impl DefaultPartialOrd for ScalarImpl {
    fn default_partial_cmp(&self, other: &ScalarImpl) -> Option<Ordering> {
        self.as_scalar_ref_impl()
            .default_partial_cmp(&other.as_scalar_ref_impl())
    }
}

impl DefaultOrd for ScalarImpl {
    fn default_cmp(&self, other: &ScalarImpl) -> Ordering {
        self.as_scalar_ref_impl()
            .default_cmp(&other.as_scalar_ref_impl())
    }
}

impl DefaultPartialOrd for ScalarRefImpl<'_> {
    fn default_partial_cmp(&self, other: &ScalarRefImpl<'_>) -> Option<Ordering> {
        default_partial_cmp_scalar_ref_impl(*self, *other)
    }
}

impl DefaultOrd for ScalarRefImpl<'_> {
    fn default_cmp(&self, other: &ScalarRefImpl<'_>) -> Ordering {
        self.default_partial_cmp(other)
            .unwrap_or_else(|| panic!("cannot compare {self:?} with {other:?}"))
    }
}

impl DefaultPartialOrd for Datum {
    fn default_partial_cmp(&self, other: &Datum) -> Option<Ordering> {
        partial_cmp_datum(self, other, OrderType::default())
    }
}

impl DefaultOrd for Datum {
    fn default_cmp(&self, other: &Datum) -> Ordering {
        cmp_datum(self, other, OrderType::default())
    }
}

impl DefaultPartialOrd for DatumRef<'_> {
    fn default_partial_cmp(&self, other: &DatumRef<'_>) -> Option<Ordering> {
        partial_cmp_datum(*self, *other, OrderType::default())
    }
}

impl DefaultOrd for DatumRef<'_> {
    fn default_cmp(&self, other: &DatumRef<'_>) -> Ordering {
        cmp_datum(*self, *other, OrderType::default())
    }
}

/// Wrapper of [`ScalarImpl`] that can be sorted in default order.
#[derive(Debug, Clone, Hash, PartialEq, Eq, EstimateSize)]
pub struct OrdScalarImpl {
    inner: ScalarImpl,
}

impl Deref for OrdScalarImpl {
    type Target = ScalarImpl;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

/// Wrapper of [`Datum`] that can be sorted in default order.
#[derive(Debug, Clone, Hash, PartialEq, Eq, EstimateSize)]
pub struct OrdDatum {
    inner: Datum,
}

impl Deref for OrdDatum {
    type Target = Datum;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

macro_rules! gen_impl_and_convert {
    ($( $inner:ident ),*) => {
        paste! {
            $(
                impl [<Ord $inner>] {
                    pub fn new(inner: $inner) -> Self {
                        Self { inner }
                    }

                    pub fn into_inner(self) -> $inner {
                        self.inner
                    }

                    pub fn as_inner(&self) -> &$inner {
                        &self.inner
                    }
                }

                impl From<$inner> for [<Ord $inner>] {
                    fn from(inner: $inner) -> Self {
                        Self::new(inner)
                    }
                }

                impl From<[<Ord $inner>]> for $inner {
                    fn from(wrapper: [<Ord $inner>]) -> Self {
                        wrapper.inner
                    }
                }

                impl PartialOrd for [<Ord $inner>] {
                    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
                        self.inner.default_partial_cmp(&other.inner)
                    }
                }

                impl Ord for [<Ord $inner>] {
                    fn cmp(&self, other: &Self) -> Ordering {
                        self.inner.default_cmp(&other.inner)
                    }
                }
            )*
        }
    };
}
gen_impl_and_convert!(ScalarImpl, Datum);
