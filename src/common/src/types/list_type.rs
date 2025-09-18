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

#[derive(Clone, PartialEq, Eq, Hash, Debug, Display, FromStr)]
#[display("{elem}[]")]
#[from_str(regex = r"(?i)^(?P<elem>.+)\[\]$")]
#[repr(transparent)]
pub struct ListType {
    elem: Box<DataType>,
}

impl ListType {
    /// Creates a new list type from the element type.
    pub fn from_elem(elem: impl Into<Box<DataType>>) -> Self {
        Self { elem: elem.into() }
    }

    /// Returns the element type of the list.
    pub fn elem(&self) -> &DataType {
        &self.elem
    }

    /// Consume `self` and returns the element type of the list.
    pub fn into_elem(self) -> DataType {
        *self.elem
    }

    /// Wrap `self` into a nested list type where the element type is `self`.
    pub fn list(self) -> Self {
        Self::from_elem(DataType::from(self))
    }
}

impl DataType {
    /// Wrap `self` into a list type where the element type is `self`.
    pub fn list(self) -> DataType {
        ListType::from_elem(self).into()
    }
}

impl From<ListType> for DataType {
    fn from(value: ListType) -> Self {
        Self::Ljst(value)
    }
}
