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

use super::{OwnedRow, Row};
use crate::types::{DefaultOrd, DefaultOrdered, DefaultPartialOrd};
use crate::util::sort_util::{cmp_datum_iter, partial_cmp_datum_iter, OrderType};

pub type OrdRow<R> = DefaultOrdered<R>;

impl<R: Row> Row for OrdRow<R> {
    type Iter<'a> = R::Iter<'a> where Self: 'a;

    deref_forward_row! {}

    fn into_owned_row(self) -> OwnedRow {
        self.into_inner().into_owned_row()
    }
}

impl<R: Row> DefaultPartialOrd for R {
    fn default_partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        partial_cmp_datum_iter(
            self.iter(),
            other.iter(),
            std::iter::repeat(OrderType::default()),
        )
    }
}

impl<R: Row> DefaultOrd for R {
    fn default_cmp(&self, other: &Self) -> std::cmp::Ordering {
        // NOTE(rc): This is slightly different from `cmp_rows`, for this function won't
        // check the length of rows.
        cmp_datum_iter(
            self.iter(),
            other.iter(),
            std::iter::repeat(OrderType::default()),
        )
    }
}
