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

use std::collections::{BTreeMap, HashMap};
use std::convert::TryFrom;
use std::num::NonZeroU32;

use itertools::Itertools;
use risingwave_common::error::{ErrorCode, RwError};
use risingwave_sqlparser::ast::{
    CreateConnectionStatement, CreateSinkStatement, CreateSourceStatement, SqlOption, Statement,
    Value,
};

mod options {
    use risingwave_common::catalog::hummock::PROPERTIES_RETENTION_SECOND_KEY;

    pub const RETENTION_SECONDS: &str = PROPERTIES_RETENTION_SECOND_KEY;
}

/// Options or properties extracted from the `WITH` clause of DDLs.
#[derive(Default, Clone, Debug, PartialEq, Eq, Hash)]
pub struct WithOptions {
    inner: BTreeMap<String, String>,
}

impl std::ops::Deref for WithOptions {
    type Target = BTreeMap<String, String>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl WithOptions {
    /// Create a new [`WithOptions`] from a [`HashMap`].
    pub fn new(inner: HashMap<String, String>) -> Self {
        Self {
            inner: inner.into_iter().collect(),
        }
    }

    /// Get the reference of the inner map.
    pub fn inner(&self) -> &BTreeMap<String, String> {
        &self.inner
    }

    pub fn inner_mut(&mut self) -> &mut BTreeMap<String, String> {
        &mut self.inner
    }

    /// Take the value of the inner map.
    pub fn into_inner(self) -> BTreeMap<String, String> {
        self.inner.into_iter().collect()
    }

    /// Parse the retention seconds from the options.
    pub fn retention_seconds(&self) -> Option<NonZeroU32> {
        self.inner
            .get(options::RETENTION_SECONDS)
            .and_then(|s| s.parse().ok())
    }

    /// Get a subset of the options from the given keys.
    pub fn subset(&self, keys: impl IntoIterator<Item = impl AsRef<str>>) -> Self {
        let inner = keys
            .into_iter()
            .filter_map(|k| {
                self.inner
                    .get_key_value(k.as_ref())
                    .map(|(k, v)| (k.clone(), v.clone()))
            })
            .collect();

        Self { inner }
    }

    /// Get the subset of the options for internal table catalogs.
    ///
    /// Currently only `retention_seconds` is included.
    pub fn internal_table_subset(&self) -> Self {
        self.subset([options::RETENTION_SECONDS])
    }

    pub fn value_eq_ignore_case(&self, key: &str, val: &str) -> bool {
        if let Some(inner_val) = self.inner.get(key) {
            if inner_val.eq_ignore_ascii_case(val) {
                return true;
            }
        }
        false
    }
}

impl TryFrom<&[SqlOption]> for WithOptions {
    type Error = RwError;

    fn try_from(options: &[SqlOption]) -> Result<Self, Self::Error> {
        let inner = options
            .iter()
            .cloned()
            .map(|x| match x.value {
                Value::SingleQuotedString(s) => Ok((x.name.real_value(), s)),
                Value::Number(n) => Ok((x.name.real_value(), n)),
                Value::Boolean(b) => Ok((x.name.real_value(), b.to_string())),
                _ => Err(ErrorCode::InvalidParameterValue(
                    "`with options` or `with properties` only support single quoted string value"
                        .to_owned(),
                )),
            })
            .try_collect()?;

        Ok(Self { inner })
    }
}

impl TryFrom<&Statement> for WithOptions {
    type Error = RwError;

    /// Extract options from the `WITH` clause from the given statement.
    fn try_from(statement: &Statement) -> Result<Self, Self::Error> {
        match statement {
            // Explain: forward to the inner statement.
            Statement::Explain { statement, .. } => Self::try_from(statement.as_ref()),

            // Table & View
            Statement::CreateTable { with_options, .. }
            | Statement::CreateView { with_options, .. } => Self::try_from(with_options.as_slice()),

            // Source & Sink
            Statement::CreateSource {
                stmt:
                    CreateSourceStatement {
                        with_properties, ..
                    },
                ..
            }
            | Statement::CreateSink {
                stmt:
                    CreateSinkStatement {
                        with_properties, ..
                    },
            }
            | Statement::CreateConnection {
                stmt:
                    CreateConnectionStatement {
                        with_properties, ..
                    },
            } => Self::try_from(with_properties.0.as_slice()),

            _ => Ok(Default::default()),
        }
    }
}
