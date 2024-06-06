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

use super::{Datum, DatumRef, ToDatumRef, ToOwnedDatum};

/// 🐮 A borrowed [`DatumRef`] or an owned [`Datum`].
#[derive(Debug, Clone)]
pub enum DatumCow<'a> {
    Ref(DatumRef<'a>),
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
        DatumCow::Ref(datum)
    }
}

impl ToDatumRef for DatumCow<'_> {
    fn to_datum_ref(&self) -> DatumRef<'_> {
        match self {
            DatumCow::Ref(datum) => *datum,
            DatumCow::Owned(datum) => datum.to_datum_ref(),
        }
    }
}

impl ToOwnedDatum for DatumCow<'_> {
    fn to_owned_datum(self) -> Datum {
        match self {
            DatumCow::Ref(datum) => datum.to_owned_datum(),
            DatumCow::Owned(datum) => datum,
        }
    }
}
