// Copyright 2025 RisingWave Labs
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

use super::{Datum, DatumRef, ToDatumRef, ToOwnedDatum};

/// 🐮 A borrowed [`DatumRef`] or an owned [`Datum`].
///
/// We do not use [`std::borrow::Cow`] because it requires the borrowed variant
/// to be a reference, whereas what we have is a [`DatumRef`] with a lifetime.
///
/// # Usage
///
/// Generally, you don't need to match on the variants of `DatumCow` to access
/// the underlying datum. Instead, you can...
///
/// - call [`to_datum_ref`](ToDatumRef::to_datum_ref) to get a borrowed
///   [`DatumRef`] without any allocation, which can be used to append to an
///   array builder or to encode into the storage representation,
///
/// - call [`to_owned_datum`](ToOwnedDatum::to_owned_datum) to get an owned
///   [`Datum`] with potentially an allocation, which can be used to store in a
///   struct without lifetime constraints.
#[derive(Debug, Clone)]
pub enum DatumCow<'a> {
    Borrowed(DatumRef<'a>),
    Owned(Datum),
}

impl PartialEq for DatumCow<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.to_datum_ref() == other.to_datum_ref()
    }
}
impl Eq for DatumCow<'_> {}

impl From<Datum> for DatumCow<'_> {
    fn from(datum: Datum) -> Self {
        DatumCow::Owned(datum)
    }
}

impl<'a> From<DatumRef<'a>> for DatumCow<'a> {
    fn from(datum: DatumRef<'a>) -> Self {
        DatumCow::Borrowed(datum)
    }
}

impl ToDatumRef for DatumCow<'_> {
    fn to_datum_ref(&self) -> DatumRef<'_> {
        match self {
            DatumCow::Borrowed(datum) => *datum,
            DatumCow::Owned(datum) => datum.to_datum_ref(),
        }
    }
}

impl ToOwnedDatum for DatumCow<'_> {
    fn to_owned_datum(self) -> Datum {
        match self {
            DatumCow::Borrowed(datum) => datum.to_owned_datum(),
            DatumCow::Owned(datum) => datum,
        }
    }
}

impl DatumCow<'_> {
    /// Equivalent to `DatumCow::Owned(Datum::None)`.
    pub const NULL: DatumCow<'static> = DatumCow::Owned(None);
}
