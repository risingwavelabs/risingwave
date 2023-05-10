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
#![allow(deprecated)]

use std::ops::Deref;

use super::{OwnedRow, Row};

/// A simple wrapper for [`OwnedRow`], which assumes that all fields are defined as `ASC` order.
/// TODO(rc): This is used by `sort_v0` and `sort_buffer_v0`, now we can remove it.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
#[deprecated(note = "The ordering of this `AscentOwnedRow` is not correct. Don't use it anymore.")]
pub struct AscentOwnedRow(OwnedRow);

impl AscentOwnedRow {
    pub fn into_inner(self) -> OwnedRow {
        self.0
    }
}

impl Deref for AscentOwnedRow {
    type Target = OwnedRow;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Row for AscentOwnedRow {
    type Iter<'a> = <OwnedRow as Row>::Iter<'a>;

    deref_forward_row! {}

    fn into_owned_row(self) -> OwnedRow {
        self.into_inner()
    }
}

impl PartialOrd for AscentOwnedRow {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.0.as_inner().partial_cmp(other.0.as_inner())
    }
}

impl Ord for AscentOwnedRow {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.partial_cmp(other)
            .unwrap_or_else(|| panic!("cannot compare rows with different types"))
    }
}

impl From<OwnedRow> for AscentOwnedRow {
    fn from(row: OwnedRow) -> Self {
        Self(row)
    }
}
