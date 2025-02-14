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
use itertools::Itertools;

use super::{DataType, DataTypeWith, FieldName};
use crate::util::iter_util::ZipEqFast;

/// A cheaply cloneable struct type.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StructType<E = FieldName>(Arc<StructTypeInner<E>>);

impl<E: Debug> Debug for StructType<E> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StructType")
            .field("fields", &self.0.fields)
            .finish()
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct StructTypeInner<E = FieldName> {
    /// The name and data type of each field.
    fields: Box<[DataTypeWith<E>]>,
    /// Whether the struct type is unnamed, i.e., "record".
    is_unnamed: bool,
}

impl<E: Clone> StructType<E> {
    pub fn map_extra<E2>(self, mut f: impl FnMut(E) -> E2) -> StructType<E2> {
        StructType(Arc::new(StructTypeInner {
            fields: (self.0.fields.iter().cloned())
                .map(|field| field.map_extra(&mut f))
                .collect(),
            is_unnamed: self.0.is_unnamed,
        }))
    }
}

impl StructType {
    /// Creates a struct type with named fields.
    pub fn new(named_fields: impl IntoIterator<Item = (impl Into<String>, DataType)>) -> Self {
        let fields = named_fields
            .into_iter()
            .map(|(name, data_type)| DataTypeWith {
                extra: FieldName(name.into()),
                data_type,
            })
            .collect();

        Self(Arc::new(StructTypeInner {
            fields,
            is_unnamed: false,
        }))
    }

    /// Creates a struct type with no fields.
    #[cfg(test)]
    pub fn empty() -> Self {
        Self::unnamed(Vec::new())
    }

    /// Creates a struct type with unnamed fields.
    pub fn unnamed(fields: Vec<DataType>) -> Self {
        let fields = fields
            .into_iter()
            .enumerate()
            .map(|(i, data_type)| DataTypeWith {
                extra: FieldName(format!("f{}", i + 1)),
                data_type,
            })
            .collect();

        Self(Arc::new(StructTypeInner {
            fields,
            is_unnamed: true,
        }))
    }

    /// Whether the struct type is unnamed, i.e., "record".
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
    /// If the struct type is unnamed, the field names will be `f1`, `f2`, etc.
    pub fn names(&self) -> impl ExactSizeIterator<Item = &str> {
        self.0.fields.iter().map(|f| f.name())
    }

    /// Gets an iterator over the types of the fields.
    pub fn types(&self) -> impl ExactSizeIterator<Item = &DataType> {
        self.0.fields.iter().map(|f| &f.data_type)
    }

    /// Gets an iterator over the fields.
    ///
    /// If the struct type is unnamed, the field names will be `f1`, `f2`, etc.
    pub fn iter(&self) -> impl ExactSizeIterator<Item = (&str, &DataType)> {
        self.0.fields.iter().map(|f| (f.name(), &f.data_type))
    }

    /// Compares the datatype with another, ignoring nested field names and metadata.
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
            // XXX: is this correct?
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
