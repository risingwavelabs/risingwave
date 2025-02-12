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

use std::fmt::Formatter;
use risingwave_pb::catalog::function::Kind;
use risingwave_pb::catalog::PbFunction;
use sea_orm::entity::prelude::*;
use sea_orm::ActiveValue::Set;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde::de::{Error, MapAccess, Visitor};
use serde::ser::SerializeStruct;
use crate::{DataType, DataTypeArray, FunctionId};

#[derive(Clone, Debug, PartialEq, Eq, EnumIter, DeriveActiveEnum, Serialize, Deserialize)]
#[sea_orm(rs_type = "String", db_type = "string(None)")]
pub enum FunctionKind {
    #[sea_orm(string_value = "Scalar")]
    Scalar,
    #[sea_orm(string_value = "Table")]
    Table,
    #[sea_orm(string_value = "Aggregate")]
    Aggregate,
}

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Eq, Serialize, Deserialize)]
#[sea_orm(table_name = "function")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub function_id: FunctionId,
    pub name: String,
    // encode Vec<String> as comma separated string
    pub arg_names: String,
    pub arg_types: DataTypeArray,
    pub return_type: DataType,
    pub language: String,
    pub runtime: Option<String>,
    pub link: Option<String>,
    pub identifier: Option<String>,
    pub body: Option<String>,
    pub compressed_binary: Option<Vec<u8>>,
    pub kind: FunctionKind,
    pub always_retry_on_network_error: bool,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::object::Entity",
        from = "Column::FunctionId",
        to = "super::object::Column::Oid",
        on_update = "NoAction",
        on_delete = "Cascade"
    )]
    Object,
}

impl Related<super::object::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Object.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}

impl From<Kind> for FunctionKind {
    fn from(kind: Kind) -> Self {
        match kind {
            Kind::Scalar(_) => Self::Scalar,
            Kind::Table(_) => Self::Table,
            Kind::Aggregate(_) => Self::Aggregate,
        }
    }
}

impl From<FunctionKind> for Kind {
    fn from(value: FunctionKind) -> Self {
        match value {
            FunctionKind::Scalar => Self::Scalar(Default::default()),
            FunctionKind::Table => Self::Table(Default::default()),
            FunctionKind::Aggregate => Self::Aggregate(Default::default()),
        }
    }
}

impl From<PbFunction> for ActiveModel {
    fn from(function: PbFunction) -> Self {
        Self {
            function_id: Set(function.id as _),
            name: Set(function.name),
            arg_names: Set(function.arg_names.join(",")),
            arg_types: Set(DataTypeArray::from(function.arg_types)),
            return_type: Set(DataType::from(&function.return_type.unwrap())),
            language: Set(function.language),
            runtime: Set(function.runtime),
            link: Set(function.link),
            identifier: Set(function.identifier),
            body: Set(function.body),
            compressed_binary: Set(function.compressed_binary),
            kind: Set(function.kind.unwrap().into()),
            always_retry_on_network_error: Set(function.always_retry_on_network_error),
        }
    }
}

const FIELDS: [&str; 13] = [
    "_id",
    "name",
    "arg_names",
    "arg_types",
    "return_type",
    "language",
    "runtime",
    "link",
    "identifier",
    "body",
    "compressed_binary",
    "kind",
    "always_retry_on_network_error",
];

pub struct MongoDb {
    function: Model
}

impl Serialize for MongoDb {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // 3 is the number of fields in the struct.
        let count = 8 + if &self.function.runtime.is_some() {1} else {0} + if &self.function.link.is_some() {1} else {0} + if &self.function.identifier.is_some() {1} else {0} + if &self.function.body.is_some() {1} else {0} + if &self.function.compressed_binary.is_some() {1} else {0};
        let mut state = serializer.serialize_struct("MongoDb", count)?;
        match &self.function.runtime {
            Some(runtime) => state.serialize_field("runtime", runtime)?,
            _ => ()
        }

        match &self.function.link {
            Some(link) => state.serialize_field("link", link)?,
            _ => ()
        }

        match &self.function.identifier {
            Some(identifier) => state.serialize_field("identifier", identifier)?,
            _ => ()
        }

        match &self.function.body {
            Some(body) => state.serialize_field("body", body)?,
            _ => ()
        }

        match &self.function.compressed_binary {
            Some(compressed_binary) => state.serialize_field("compressed_binary", compressed_binary)?,
            _ => ()
        }
        state.serialize_field("_id", &self.function.function_id)?;
        state.serialize_field("name", &self.function.name)?;
        state.serialize_field("arg_names", &self.function.arg_names)?;
        state.serialize_field("arg_types", &self.function.arg_types)?;
        state.serialize_field("return_type", &self.function.return_type)?;
        state.serialize_field("language", &self.function.language)?;
        state.serialize_field("kind", &self.function.kind)?;
        state.serialize_field("always_retry_on_network_error", &self.function.always_retry_on_network_error)?;
        state.end()
    }
}

impl<'de> Deserialize<'de> for MongoDb {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct MongoDbVisitor;
        impl<'de> Visitor<'de> for MongoDbVisitor {
            type Value = MongoDb;

            fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
                formatter.write_str("MongoDb")
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut function_id: Option<FunctionId> = None;
                let mut name: Option<String> = None;
                let mut arg_names: Option<String> = None;
                let mut arg_types: Option<DataTypeArray> = None;
                let mut return_type: Option<DataType> = None;
                let mut language: Option<String> = None;
                let mut runtime: Option<String> = None;
                let mut link: Option<String> = None;
                let mut identifier: Option<String> = None;
                let mut body: Option<String> = None;
                let mut compressed_binary: Option<Vec<u8>> = None;
                let mut kind: Option<FunctionKind> = None;
                let mut always_retry_on_network_error: Option<bool> = None;
                while let Some((key, value)) = map.next_entry()? {
                    match key {
                        "_id" => function_id = Some(i32::deserialize(value).unwrap()),
                        "name" => name = Some(value.to_string()),
                        "arg_names" => arg_names = Some(value.to_string()),
                        "arg_types" => arg_types = Some(DataTypeArray::deserialize(value).unwrap()),
                        "return_type" => return_type = Some(DataType::deserialize(value).unwrap()),
                        "language" => language = Some(value.to_string()),
                        "runtime" => runtime = Some(value.to_string()),
                        "link" => link = Some(value.to_string()),
                        "identifier" => identifier = Some(value.to_string()),
                        "body" => body = Some(value.to_string()),
                        "compressed_binary" => compressed_binary = Some(Vec::deserialize(value).unwrap()),
                        "kind" => name = Some(value.to_string()),
                        "always_retry_on_network_error" => always_retry_on_network_error = Some(bool::deserialize(value).unwrap()),
                        x => return Err(Error::unknown_field(x, &FIELDS)),
                    }
                }

                let function = Model {
                    function_id: function_id.ok_or_else(|| Error::missing_field("_id"))?,
                    name: name.ok_or_else(|| Error::missing_field("name"))?,
                    arg_names: arg_names.ok_or_else(|| Error::missing_field("arg_names"))?,
                    arg_types: arg_types.ok_or_else(|| Error::missing_field("arg_types"))?,
                    return_type: return_type.ok_or_else(|| Error::missing_field("return_type"))?,
                    language: language.ok_or_else(|| Error::missing_field("language"))?,
                    runtime,
                    link,
                    identifier,
                    body,
                    compressed_binary,
                    kind: kind.ok_or_else(|| Error::missing_field("kind"))?,
                    always_retry_on_network_error: always_retry_on_network_error.ok_or_else(|| Error::missing_field("always_retry_on_network_error"))?,
                };
                Ok(Self::Value {function})
            }
        }
        deserializer.deserialize_map(MongoDbVisitor {})
    }
}
