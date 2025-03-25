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

use std::fmt::{Debug, Display, Formatter};
use std::str::FromStr;
use std::sync::Arc;

use anyhow::anyhow;
use either::Either;
use itertools::{Itertools, repeat_n};

use super::DataType;
use crate::catalog::ColumnId;
use crate::util::iter_util::ZipEqFast;

/// A cheaply cloneable struct type.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StructType(Arc<StructTypeInner>);

impl Debug for StructType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let alternate = f.alternate();

        let mut d = f.debug_struct("StructType");
        d.field("fields", &self.0.fields);
        if let Some(ids) = &self.0.field_ids
        // TODO: This is for making `EXPLAIN` output more concise, but it hurts the readability
        // for testing and debugging. Avoid using `Debug` repr in `EXPLAIN` output instead.
            && alternate
        {
            d.field("field_ids", ids);
        }
        d.finish()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct StructTypeInner {
    /// The name and data type of each field.
    ///
    /// If fields are unnamed, the names will be `f1`, `f2`, etc.
    fields: Box<[(String, DataType)]>,

    /// The ids of the fields. Used in serialization for nested-schema evolution purposes.
    ///
    /// Only present if this data type is persisted within a table schema (`ColumnDesc`)
    /// in a new version of the catalog that supports nested-schema evolution.
    field_ids: Option<Box<[ColumnId]>>,

    /// Whether the fields are unnamed.
    is_unnamed: bool,
}

impl StructType {
    /// Creates a struct type with named fields.
    pub fn new(named_fields: impl IntoIterator<Item = (impl Into<String>, DataType)>) -> Self {
        let fields = named_fields
            .into_iter()
            .map(|(name, ty)| (name.into(), ty))
            .collect();

        Self(Arc::new(StructTypeInner {
            fields,
            field_ids: None,
            is_unnamed: false,
        }))
    }

    /// Creates a struct type with no fields. This makes no sense in practice.
    #[cfg(test)]
    pub fn empty() -> Self {
        Self::unnamed(Vec::new())
    }

    /// Creates a struct type with unnamed fields. The names will be assigned `f1`, `f2`, etc.
    pub fn unnamed(fields: Vec<DataType>) -> Self {
        let fields = fields
            .into_iter()
            .enumerate()
            .map(|(i, ty)| (format!("f{}", i + 1), ty))
            .collect();

        Self(Arc::new(StructTypeInner {
            fields,
            field_ids: None,
            is_unnamed: true,
        }))
    }

    /// Attaches given field ids to the struct type.
    pub fn with_ids(self, ids: impl IntoIterator<Item = ColumnId>) -> Self {
        let ids: Box<[ColumnId]> = ids.into_iter().collect();

        assert_eq!(ids.len(), self.len(), "ids length mismatches");
        assert!(
            ids.iter().all(|id| *id != ColumnId::placeholder()),
            "ids should not contain placeholder value"
        );

        let mut inner = Arc::unwrap_or_clone(self.0);
        inner.field_ids = Some(ids);
        Self(Arc::new(inner))
    }

    /// Whether the struct type has field ids.
    ///
    /// Note that this does not recursively check whether composite fields have ids.
    pub fn has_ids(&self) -> bool {
        self.0.field_ids.is_some()
    }

    /// Whether the fields are unnamed.
    pub fn is_unnamed(&self) -> bool {
        self.0.is_unnamed
    }

    /// Returns the number of fields.
    pub fn len(&self) -> usize {
        self.0.fields.len()
    }

    /// Returns `true` if there are no fields.
    pub fn is_empty(&self) -> bool {
        self.0.fields.is_empty()
    }

    /// Gets an iterator over the names of the fields.
    ///
    /// If fields are unnamed, the field names will be `f1`, `f2`, etc.
    pub fn names(&self) -> impl ExactSizeIterator<Item = &str> {
        self.0.fields.iter().map(|(name, _)| name.as_str())
    }

    /// Gets an iterator over the types of the fields.
    pub fn types(&self) -> impl ExactSizeIterator<Item = &DataType> {
        self.0.fields.iter().map(|(_, ty)| ty)
    }

    /// Gets the type of a field by index.
    pub fn type_at(&self, index: usize) -> &DataType {
        &self.0.fields[index].1
    }

    /// Gets an iterator over the fields.
    ///
    /// If fields are unnamed, the field names will be `f1`, `f2`, etc.
    pub fn iter(&self) -> impl ExactSizeIterator<Item = (&str, &DataType)> {
        self.0.fields.iter().map(|(name, ty)| (name.as_str(), ty))
    }

    /// Gets an iterator over the field ids.
    ///
    /// Returns `None` if they are not present. See documentation on the field `field_ids`
    /// for the cases.
    pub fn ids(&self) -> Option<impl ExactSizeIterator<Item = ColumnId> + '_> {
        self.0.field_ids.as_ref().map(|ids| ids.iter().copied())
    }

    /// Get an iterator over the field ids, or a sequence of placeholder ids if they are not present.
    pub fn ids_or_placeholder(&self) -> impl ExactSizeIterator<Item = ColumnId> + '_ {
        match self.ids() {
            Some(ids) => Either::Left(ids),
            None => Either::Right(repeat_n(ColumnId::placeholder(), self.len())),
        }
    }

    /// Compares the datatype with another, ignoring nested field names and ids.
    pub fn equals_datatype(&self, other: &StructType) -> bool {
        if self.len() != other.len() {
            return false;
        }

        (self.types())
            .zip_eq_fast(other.types())
            .all(|(a, b)| a.equals_datatype(b))
    }
}

impl Display for StructType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if self.is_unnamed() {
            // To be consistent with the return type of `ROW` in Postgres.
            write!(f, "record")
        } else {
            write!(
                f,
                "struct<{}>",
                self.iter()
                    .map(|(name, ty)| format!("{} {}", name, ty))
                    .join(", ")
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
        if !(s.starts_with("struct<") && s.ends_with('>')) {
            return Err(anyhow!("expect struct<...>"));
        };
        let mut fields = Vec::new();
        for field in s[7..s.len() - 1].split(',') {
            let field = field.trim();
            let mut iter = field.split_whitespace();
            let field_name = iter.next().unwrap().to_owned();
            let field_type = DataType::from_str(iter.next().unwrap())?;
            fields.push((field_name, field_type));
        }
        Ok(Self::new(fields))
    }
}
