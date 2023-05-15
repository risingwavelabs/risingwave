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

use paste::paste;
use risingwave_common_proc_macro::EstimateSize;

use crate::estimate_size::EstimateSize;
use crate::for_all_scalar_variants;
use crate::types::{Datum, DatumRef, ScalarImpl, ScalarRefImpl};
use crate::util::sort_util::{cmp_datum, partial_cmp_datum, OrderType};

#[derive(Debug, Clone, Hash, PartialEq, Eq, EstimateSize)]
pub struct OrdScalarImpl {
    inner: ScalarImpl,
}

impl OrdScalarImpl {
    pub fn as_scalar_ref_impl(&self) -> OrdScalarRefImpl<'_> {
        OrdScalarRefImpl {
            inner: self.inner.as_scalar_ref_impl(),
        }
    }
}

#[derive(Debug, Copy, Clone, Hash, PartialEq, Eq)]
pub struct OrdScalarRefImpl<'a> {
    inner: ScalarRefImpl<'a>,
}

impl<'a> OrdScalarRefImpl<'a> {
    pub fn into_scalar_impl(self) -> OrdScalarImpl {
        OrdScalarImpl {
            inner: self.inner.into_scalar_impl(),
        }
    }
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, EstimateSize)]
pub struct OrdDatum {
    inner: Datum,
}

#[derive(Debug, Copy, Clone, Hash, PartialEq, Eq)]
pub struct OrdDatumRef<'a> {
    inner: DatumRef<'a>,
}

macro_rules! gen_new_and_convert {
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
            )*
        }
    };
    ($( $inner:ident<$lt:lifetime> ),*) => {
        paste! {
            $(
                impl<$lt> [<Ord $inner>]<$lt> {
                    pub fn new(inner: $inner<$lt>) -> Self {
                        Self { inner }
                    }

                    pub fn into_inner(self) -> $inner<$lt> {
                        self.inner
                    }
                }

                impl<$lt> From<$inner<$lt>> for [<Ord $inner>]<$lt> {
                    fn from(inner: $inner<$lt>) -> Self {
                        Self::new(inner)
                    }
                }

                impl<$lt> From<[<Ord $inner>]<$lt>> for $inner<$lt> {
                    fn from(wrapper: [<Ord $inner>]<$lt>) -> Self {
                        wrapper.inner
                    }
                }
            )*
        }
    };
}
gen_new_and_convert!(ScalarImpl, Datum);
gen_new_and_convert!(ScalarRefImpl<'a>, DatumRef<'a>);

macro_rules! gen_partial_cmp_scalar_ref_impl {
    ($( { $variant_name:ident, $suffix_name:ident, $scalar:ty, $scalar_ref:ty } ),*) => {
        fn partial_cmp_scalar_ref_impl(lhs: ScalarRefImpl<'_>, rhs: ScalarRefImpl<'_>) -> Option<Ordering> {
            match (lhs, rhs) {
                $((ScalarRefImpl::$variant_name(lhs_inner), ScalarRefImpl::$variant_name(ref rhs_inner)) => Some(lhs_inner.cmp(rhs_inner)),)*
                _ => None,
            }
        }
    };
}
for_all_scalar_variants!(gen_partial_cmp_scalar_ref_impl);

impl PartialOrd for OrdScalarImpl {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        partial_cmp_scalar_ref_impl(
            self.inner.as_scalar_ref_impl(),
            other.inner.as_scalar_ref_impl(),
        )
    }
}

impl PartialOrd for OrdScalarRefImpl<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        partial_cmp_scalar_ref_impl(self.inner, other.inner)
    }
}

impl PartialOrd for OrdDatum {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        partial_cmp_datum(&self.inner, &other.inner, OrderType::default())
    }
}

impl PartialOrd for OrdDatumRef<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        partial_cmp_datum(self.inner, other.inner, OrderType::default())
    }
}

macro_rules! gen_ord {
    ($($wrapper:ty),*) => {
        $(
            impl Ord for $wrapper {
                fn cmp(&self, other: &Self) -> Ordering {
                    self.partial_cmp(other)
                        .unwrap_or_else(|| panic!("cannot compare {self:?} with {other:?}"))
                }
            }
        )*
    };
}
gen_ord!(
    OrdScalarImpl,
    OrdScalarRefImpl<'_>,
    OrdDatum,
    OrdDatumRef<'_>
);

pub trait DefaultPartialOrd {
    fn default_partial_cmp(&self, other: &Self) -> Option<Ordering>;
}

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
        OrdScalarRefImpl::new(*self).partial_cmp(&OrdScalarRefImpl::new(*other))
    }
}

impl DefaultOrd for ScalarRefImpl<'_> {
    fn default_cmp(&self, other: &ScalarRefImpl<'_>) -> Ordering {
        OrdScalarRefImpl::new(*self).cmp(&OrdScalarRefImpl::new(*other))
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
