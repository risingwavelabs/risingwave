use std::collections::HashMap;
use std::convert::TryFrom;

use itertools::Itertools;
use risingwave_common::error::{ErrorCode, RwError};
use risingwave_sqlparser::ast::{
    CreateSinkStatement, CreateSourceStatement, SqlOption, Statement, Value,
};

use crate::catalog::source_catalog::KAFKA_CONNECTOR;

mod options {
    use risingwave_common::catalog::hummock::PROPERTIES_RETAINTION_SECOND_KEY;

    pub const APPEND_ONLY: &str = "appendonly";
    pub const CONNECTOR: &str = "connector";
    pub const RETENTION_SECONDS: &str = PROPERTIES_RETAINTION_SECOND_KEY;
}

#[derive(Default, Clone, Debug)]
pub struct WithOptions {
    inner: HashMap<String, String>,
}

impl WithOptions {
    pub fn new(inner: HashMap<String, String>) -> Self {
        Self { inner }
    }

    pub fn inner(&self) -> &HashMap<String, String> {
        &self.inner
    }

    pub fn retention_seconds(&self) -> Option<u32> {
        self.inner
            .get(options::RETENTION_SECONDS)
            .and_then(|s| s.parse().ok())
    }

    pub fn append_only(&self) -> bool {
        if let Some(val) = self.inner.get(options::APPEND_ONLY) {
            if val.eq_ignore_ascii_case("true") {
                return true;
            }
        }
        if let Some(val) = self.inner.get(options::CONNECTOR) {
            // Kafka source is append-only
            if val.eq_ignore_ascii_case(KAFKA_CONNECTOR) {
                return true;
            }
        }
        false
    }

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

    pub fn internal_table_subset(&self) -> Self {
        self.subset([options::RETENTION_SECONDS])
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

    fn try_from(value: &Statement) -> Result<Self, Self::Error> {
        match value {
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
            } => Self::try_from(with_properties.0.as_slice()),

            _ => Ok(Default::default()),
        }
    }
}
