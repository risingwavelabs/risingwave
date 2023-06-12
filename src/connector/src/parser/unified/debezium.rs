use risingwave_common::types::{DataType, Datum, ScalarImpl};

use super::{Access, ChangeEvent, ChangeEventOperation};

pub struct DebeziumChangeEvent<A> {
    value_accessor: Option<A>,
    key_accessor: Option<A>,
}

const BEFORE: &str = "before";
const AFTER: &str = "after";
const OP: &str = "op";

pub const DEBEZIUM_READ_OP: &str = "r";
pub const DEBEZIUM_CREATE_OP: &str = "c";
pub const DEBEZIUM_UPDATE_OP: &str = "u";
pub const DEBEZIUM_DELETE_OP: &str = "d";

impl<A> DebeziumChangeEvent<A>
where
    A: Access,
{
    pub fn with_value(value_accessor: A) -> Self {
        Self::new(None, Some(value_accessor))
    }

    pub fn with_key(key_accessor: A) -> Self {
        Self::new(Some(key_accessor), None)
    }

    /// Panic: one of the `key_accessor` or `value_accessor` must be provided.
    fn new(key_accessor: Option<A>, value_accessor: Option<A>) -> Self {
        assert!(key_accessor.is_some() || value_accessor.is_some());
        Self {
            value_accessor,
            key_accessor,
        }
    }
}

impl<A> ChangeEvent for DebeziumChangeEvent<A>
where
    A: Access,
{
    fn access_field(
        &self,
        name: &str,
        type_expected: &risingwave_common::types::DataType,
    ) -> super::AccessResult {
        match self.op()? {
            ChangeEventOperation::Delete => {
                if let Some(va) = self.value_accessor.as_ref() {
                    va.access(&[BEFORE, name], Some(type_expected))
                } else {
                    self.key_accessor
                        .as_ref()
                        .unwrap()
                        .access(&[name], Some(type_expected))
                }
            }

            // value should not be None.
            ChangeEventOperation::Upsert => self
                .value_accessor
                .as_ref()
                .unwrap()
                .access(&[AFTER, name], Some(type_expected)),
        }
    }

    fn op(&self) -> std::result::Result<ChangeEventOperation, super::AccessError> {
        if let Some(accessor) = &self.value_accessor {
            if let Some(ScalarImpl::Utf8(op)) = accessor.access(&[OP], Some(&DataType::Varchar))? {
                match op.as_ref() {
                    DEBEZIUM_READ_OP | DEBEZIUM_CREATE_OP | DEBEZIUM_UPDATE_OP => {
                        return Ok(ChangeEventOperation::Upsert)
                    }
                    DEBEZIUM_DELETE_OP => return Ok(ChangeEventOperation::Delete),
                    _ => (),
                }
            }
            Err(super::AccessError::Undefined {
                name: "op".into(),
                path: Default::default(),
            })
        } else {
            Ok(ChangeEventOperation::Delete)
        }
    }
}

pub struct MongoProjeciton<A> {
    accessor: A,
}

pub fn extract_bson_id(id_type: &DataType, bson_doc: &serde_json::Value) -> anyhow::Result<Datum> {
    let id_field = bson_doc
        .get("_id")
        .ok_or_else(|| anyhow::format_err!("Debezuim Mongo requires document has a `_id` field"))?;
    let id: Datum = match id_type {
    DataType::Jsonb => ScalarImpl::Jsonb(id_field.clone().into()).into(),
    DataType::Varchar => match id_field {
        serde_json::Value::String(s) => Some(ScalarImpl::Utf8(s.clone().into())),
        serde_json::Value::Object(obj) if obj.contains_key("$oid") => Some(ScalarImpl::Utf8(
            obj["$oid"].as_str().to_owned().unwrap_or_default().into(),
        )),
        _ =>  anyhow::bail!(
            "Can not convert bson {:?} to {:?}",
            id_field, id_type
        ),
    },
    DataType::Int32 => {
        if let serde_json::Value::Object(ref obj) = id_field && obj.contains_key("$numberInt") {
            let int_str = obj["$numberInt"].as_str().unwrap_or_default();
            Some(ScalarImpl::Int32(int_str.parse().unwrap_or_default()))
        } else {
            anyhow::bail!(
                "Can not convert bson {:?} to {:?}",
                id_field, id_type
            )
        }
    }
    DataType::Int64 => {
        if let serde_json::Value::Object(ref obj) = id_field && obj.contains_key("$numberLong")
        {
            let int_str = obj["$numberLong"].as_str().unwrap_or_default();
            Some(ScalarImpl::Int64(int_str.parse().unwrap_or_default()))
        } else {
            anyhow::bail!(
                "Can not convert bson {:?} to {:?}",
                id_field, id_type
            )
        }
    }
    _ => unreachable!("DebeziumMongoJsonParser::new must ensure _id column datatypes."),
};
    Ok(id)
}
impl<A> MongoProjeciton<A> {
    pub fn new(accessor: A) -> Self {
        Self { accessor }
    }
}

impl<A> Access for MongoProjeciton<A>
where
    A: Access,
{
    fn access(&self, path: &[&str], type_expected: Option<&DataType>) -> super::AccessResult {
        match path {
            ["after" | "before", "_id"] => {
                let payload = self.access(&[path[0]], Some(&DataType::Jsonb))?;
                if let Some(ScalarImpl::Jsonb(bson_doc)) = payload {
                    Ok(extract_bson_id(
                        type_expected.unwrap_or(&DataType::Jsonb),
                        &bson_doc.take(),
                    )?)
                } else {
                    unreachable!("the result of access must match the type_expected")
                }
            }
            ["after" | "before", "payload"] => self.access(&[path[0]], Some(&DataType::Jsonb)),
            _ => self.accessor.access(path, type_expected),
        }
    }
}
