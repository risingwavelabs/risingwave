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

use std::fmt::{Display, Formatter};
use std::str::FromStr;
use std::sync::Arc;

use itertools::Itertools;

use super::DataType;
use crate::util::iter_util::{ZipEqDebug, ZipEqFast};

/// A cheaply cloneable struct type.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StructType {
    // Details about a struct type. There are 2 cases for a struct:
    // 1. `field_names.len() == field_types.len()`: it represents a struct with named fields,
    //     e.g. `STRUCT<i INT, j VARCHAR>`.
    // 2. `field_names.len() == 0`: it represents a struct with unnamed fields,
    //     e.g. `ROW(1, 2)`.
    pub(super) field_names: Arc<[String]>,
    pub(super) field_types: Arc<[DataType]>,
}

impl StructType {
    /// Creates a struct type with named fields.
    pub fn new(named_fields: Vec<(impl Into<String>, DataType)>) -> Self {
        let mut field_types = Vec::with_capacity(named_fields.len());
        let mut field_names = Vec::with_capacity(named_fields.len());
        for (name, ty) in named_fields {
            field_names.push(name.into());
            field_types.push(ty);
        }
        Self {
            field_types: field_types.into(),
            field_names: field_names.into(),
        }
    }

    /// Creates a struct type with no fields.
    #[cfg(test)]
    pub fn empty() -> Self {
        Self {
            field_types: Arc::new([]),
            field_names: Arc::new([]),
        }
    }

    /// Creates a struct type with unnamed fields.
    pub fn unnamed(fields: Vec<DataType>) -> Self {
        Self {
            field_types: fields.into(),
            field_names: Arc::new([]),
        }
    }

    /// Returns the number of fields.
    pub fn len(&self) -> usize {
        self.field_types.len()
    }

    /// Returns `true` if there are no fields.
    pub fn is_empty(&self) -> bool {
        self.field_types.is_empty()
    }

    /// Gets an iterator over the names of the fields.
    ///
    /// If the struct field is unnamed, the iterator returns **no names**.
    pub fn names(&self) -> impl ExactSizeIterator<Item = &str> {
        self.field_names.iter().map(|s| s.as_str())
    }

    /// Gets an iterator over the types of the fields.
    pub fn types(&self) -> impl ExactSizeIterator<Item = &DataType> {
        self.field_types.iter()
    }

    /// Gets an iterator over the fields.
    ///
    /// If the struct field is unnamed, the iterator returns **empty strings**.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &DataType)> {
        self.field_names
            .iter()
            .map(|s| s.as_str())
            .chain(std::iter::repeat("").take(self.field_types.len() - self.field_names.len()))
            .zip_eq_debug(self.field_types.iter())
    }
}

impl Display for StructType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if self.field_names.is_empty() {
            write!(f, "record")
        } else {
            write!(
                f,
                "struct<{}>",
                (self.field_types.iter())
                    .zip_eq_fast(self.field_names.iter())
                    .map(|(d, s)| format!("{} {}", s, d))
                    .join(",")
            )
        }
    }
}

impl FromStr for StructType {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s == "record" {
            return Ok(StructType::unnamed(Vec::new()));
        }
        let s = s.trim_start_matches("struct<").trim_end_matches('>');
        let mut field_types = Vec::new();
        let mut field_names = Vec::new();
        for field in s.split(',') {
            let field = field.trim();
            let mut iter = field.split_whitespace();
            let field_name = iter.next().unwrap();
            let field_type = iter.next().unwrap();
            field_names.push(field_name.to_string());
            field_types.push(DataType::from_str(field_type)?);
        }
        Ok(StructType {
            field_types: field_types.into(),
            field_names: field_names.into(),
        })
    }
}
