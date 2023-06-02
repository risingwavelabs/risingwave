use risingwave_common::types::DataType;

use super::{Access, OperateRow, RowOperation};
use crate::parser::unified::AccessError;

pub struct UpsertAccess<K, V> {
    pub key_accessor: Option<K>,
    pub value_accessor: Option<V>,
    pub primary_key_column_name: String,
}

impl<K, V> Access for UpsertAccess<K, V>
where
    K: Access,
    V: Access,
{
    fn access(&self, path: &[&str], shape: Option<&DataType>) -> super::AccessResult {
        let create_error = |name: String| AccessError::Undefined {
            name: name,
            path: String::new(),
        };
        match path.first() {
            Some(&"key") => {
                if let Some(ka) = &self.key_accessor {
                    ka.access(&path[1..], shape)
                } else {
                    Err(create_error("key".to_string()))
                }
            }
            Some(&"value") => {
                if let Some(va) = &self.value_accessor {
                    va.access(&path[1..], shape)
                } else {
                    Err(create_error("value".to_string()))
                }
            }
            None => Ok(None),
            Some(other) => Err(create_error(other.to_string())),
        }
    }
}

impl<K, V> OperateRow for UpsertAccess<K, V>
where
    K: Access,
    V: Access,
{
    fn op(&self) -> std::result::Result<RowOperation, AccessError> {
        if let Ok(Some(_)) = self.access(&["value"], None) {
            Ok(RowOperation::Insert)
        } else {
            Ok(RowOperation::Delete)
        }
    }

    fn access_field(&self, name: &str, shape: &DataType) -> super::AccessResult {
        // access value firstly
        match self.access(&["value", name], Some(shape)) {
            Err(AccessError::Undefined { .. }) => (), // fallthrough
            other => return other,
        };

        match self.access(&["key", name], Some(shape)) {
            Err(AccessError::Undefined { .. }) => (), // fallthrough
            other => return other,
        };

        if name == self.primary_key_column_name {
            return self.access(&["key"], Some(shape));
        }

        Ok(None)
    }

    fn access_before(&self, _name: &str, _shape: &DataType) -> super::AccessResult {
        unreachable!("upsert access never do update op, so this method should nerver be called")
    }
}
