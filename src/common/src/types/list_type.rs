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

use parse_display::{Display, FromStr};

use crate::types::DataType;

/// A list type, or `ARRAY` type in PostgreSQL.
#[derive(Clone, PartialEq, Eq, Hash, Display, FromStr)]
#[display("{elem}[]")]
#[from_str(regex = r"(?i)^(?P<elem>.+)\[\]$")]
pub struct ListType {
    /// The element type of the list.
    elem: Box<DataType>,
}

// TODO(list): debug impl of list is made transparent to maintain the old output.
// We may change this if it's found confusing.
impl std::fmt::Debug for ListType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.elem().fmt(f)
    }
}

impl ListType {
    /// Creates a list type with the element type.
    pub fn new(elem: DataType) -> Self {
        Self {
            elem: Box::new(elem),
        }
    }

    /// Returns the element type of the list.
    pub fn elem(&self) -> &DataType {
        &self.elem
    }

    /// Consume `self` and returns the element type of the list.
    pub fn into_elem(self) -> DataType {
        *self.elem
    }
}

impl DataType {
    /// Wrap `self` into a list type where the element type is `self`.
    pub fn list(self) -> DataType {
        ListType::new(self).into()
    }
}

impl From<ListType> for DataType {
    fn from(value: ListType) -> Self {
        Self::List(value)
    }
}

// For parsing `elem: Box<DataType>`
impl std::str::FromStr for Box<DataType> {
    type Err = <DataType as std::str::FromStr>::Err;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        DataType::from_str(s).map(Box::new)
    }
}
