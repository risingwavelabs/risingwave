use risingwave_common::types::DataType;

use super::{Access, ChangeEvent, ChangeEventOperation};
use crate::parser::unified::AccessError;

/// `UpsertAccess` wraps a key-value message format into an upsert source.
/// A key accessor and a value accessor are required.
pub struct UpsertAccess<K, V> {
    key_accessor: Option<K>,
    value_accessor: Option<V>,
    primary_key_column_name: Option<String>,
}

impl<K, V> Default for UpsertAccess<K, V> {
    fn default() -> Self {
        Self {
            key_accessor: None,
            value_accessor: None,
            primary_key_column_name: None,
        }
    }
}

impl<K, V> UpsertAccess<K, V> {
    pub fn with_key(mut self, key: K) -> Self
    where
        K: Access,
    {
        self.key_accessor = Some(key);
        self
    }

    pub fn with_value(mut self, value: V) -> Self
    where
        V: Access,
    {
        self.value_accessor = Some(value);
        self
    }

    pub fn with_primary_key_column_name(mut self, name: impl ToString) -> Self {
        self.primary_key_column_name = Some(name.to_string());
        self
    }
}

impl<K, V> Access for UpsertAccess<K, V>
where
    K: Access,
    V: Access,
{
    fn access(&self, path: &[&str], type_expected: Option<&DataType>) -> super::AccessResult {
        let create_error = |name: String| AccessError::Undefined {
            name,
            path: String::new(),
        };
        match path.first() {
            Some(&"key") => {
                if let Some(ka) = &self.key_accessor {
                    ka.access(&path[1..], type_expected)
                } else {
                    Err(create_error("key".to_string()))
                }
            }
            Some(&"value") => {
                if let Some(va) = &self.value_accessor {
                    va.access(&path[1..], type_expected)
                } else {
                    Err(create_error("value".to_string()))
                }
            }
            None => Ok(None),
            Some(other) => Err(create_error(other.to_string())),
        }
    }
}

impl<K, V> ChangeEvent for UpsertAccess<K, V>
where
    K: Access,
    V: Access,
{
    fn op(&self) -> std::result::Result<ChangeEventOperation, AccessError> {
        if let Ok(Some(_)) = self.access(&["value"], None) {
            Ok(ChangeEventOperation::Upsert)
        } else {
            Ok(ChangeEventOperation::Delete)
        }
    }

    fn access_field(&self, name: &str, type_expected: &DataType) -> super::AccessResult {
        // access value firstly
        match self.access(&["value", name], Some(type_expected)) {
            Err(AccessError::Undefined { .. }) => (), // fallthrough
            other => return other,
        };

        match self.access(&["key", name], Some(type_expected)) {
            Err(AccessError::Undefined { .. }) => (), // fallthrough
            other => return other,
        };

        if let Some(primary_key_column_name) = &self.primary_key_column_name && name == primary_key_column_name {
            return self.access(&["key"], Some(type_expected));
        }

        Ok(None)
    }
}
