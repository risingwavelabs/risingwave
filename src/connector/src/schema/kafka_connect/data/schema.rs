// Copyright 2024 RisingWave Labs
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

//! This module is an analogy to Java package `org.apache.kafka.connect.data`.
//! <https://github.com/apache/kafka/blob/trunk/connect/api/src/main/java/org/apache/kafka/connect/data/ConnectSchema.java>

use std::sync::LazyLock;

use simd_json::prelude::ValueAsContainer;
use simd_json::BorrowedValue;

/// Get a value from a json object by key, case insensitive.
///
/// Returns `None` if the given json value is not an object, or the key is not found.
#[derive(Debug, Clone)]
pub enum ConnectSchema {
    Primitive(PrimitiveSchema),
    Array(ArraySchema),
    Map(MapSchema),
    Struct(StructSchema),
}

impl ConnectSchema {
    pub fn base(&self) -> &SchemaBase {
        match self {
            ConnectSchema::Primitive(s) => &s.base,
            ConnectSchema::Array(s) => &s.base,
            ConnectSchema::Map(s) => &s.base,
            ConnectSchema::Struct(s) => &s.base,
        }
    }

    pub fn equals(&self, _other: &Self) -> bool {
        todo!()
    }
}

#[derive(Debug, Clone)]
pub struct SchemaBase {
    type_: SchemaType,
    optional: bool,
    default: Option<Value>,
    name: Option<Box<str>>,
    version: Option<i32>,
    doc: Option<Box<str>>,
    /// For logical types, e.g. `decimal` with `scale`.
    parameters: std::collections::HashMap<String, String>,
}

#[derive(Debug, Clone)]
pub struct PrimitiveSchema {
    base: SchemaBase,
}

#[derive(Debug, Clone)]
pub struct ArraySchema {
    base: SchemaBase,
    val_schema: Box<ConnectSchema>,
}

#[derive(Debug, Clone)]
pub struct MapSchema {
    base: SchemaBase,
    key_schema: Box<ConnectSchema>,
    val_schema: Box<ConnectSchema>,
}

#[derive(Debug, Clone)]
pub struct StructSchema {
    base: SchemaBase,
    fields: Vec<Field>, // java: LinkedHashMap
    fields_by_name: std::collections::BTreeMap<String, usize>,
}

#[derive(Debug, Clone /* , PartialEq, Eq, Hash */)]
pub struct Field {
    name: String,
    index: usize,
    schema: ConnectSchema,
}

impl StructSchema {
    pub fn new(base: SchemaBase, fields: Vec<Field>) -> Self {
        let fields_by_name = fields
            .iter()
            .enumerate()
            .map(|(i, f)| (f.name.clone(), i))
            .collect();
        Self {
            base,
            fields,
            fields_by_name,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SchemaType {
    Int8,
    Int16,
    Int32,
    Int64,
    Float32,
    Float64,
    Boolean,
    String,
    Bytes,
    Array,
    Map,
    Struct,
}

impl SchemaType {
    pub fn name(self) -> &'static str {
        todo!()
    }

    pub fn is_primitive(self) -> bool {
        match self {
            SchemaType::Int8
            | SchemaType::Int16
            | SchemaType::Int32
            | SchemaType::Int64
            | SchemaType::Float32
            | SchemaType::Float64
            | SchemaType::Boolean
            | SchemaType::String
            | SchemaType::Bytes => true,

            SchemaType::Array | SchemaType::Map | SchemaType::Struct => false,
        }
    }
}

impl std::fmt::Display for SchemaType {
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum Value {
    Struct(Struct),
    List(Vec<Option<Value>>),
    Map(std::collections::HashMap<Value, Value>),
}

#[derive(Debug, Clone)]
pub struct Struct;

impl Struct {
    pub fn schema(&self) -> ConnectSchema {
        todo!()
    }

    pub fn validate(&self) -> Result<(), String> {
        todo!()
    }
}

/// Rust `TypeId`?
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Class {
    Byte,
    Short,
    Integer,
    Long,
    Float,
    Double,
    Boolean,
    String,
    SliceOfByte,
    ByteBuffer,
    List,
    Map,
    Struct,
    BigDecimal,
    Date, // `java.util.Date` shall be replaced by `java.time.*`
}

fn is_instance(_class: &Class, _value: &Value) -> bool {
    todo!()
}

static SCHEMA_TYPE_CLASSES: LazyLock<std::collections::HashMap<SchemaType, Box<[Class]>>> =
    LazyLock::new(|| {
        let mut m = std::collections::HashMap::default();
        m.insert(SchemaType::Int8, [Class::Byte].into());
        m.insert(SchemaType::Int16, [Class::Short].into());
        m.insert(SchemaType::Int32, [Class::Integer].into());
        m.insert(SchemaType::Int64, [Class::Long].into());
        m.insert(SchemaType::Float32, [Class::Float].into());
        m.insert(SchemaType::Float64, [Class::Double].into());
        m.insert(SchemaType::Boolean, [Class::Boolean].into());
        m.insert(SchemaType::String, [Class::String].into());
        m.insert(
            SchemaType::Bytes,
            [Class::SliceOfByte, Class::ByteBuffer].into(),
        );
        m.insert(SchemaType::Array, [Class::List].into());
        m.insert(SchemaType::Map, [Class::Map].into());
        m.insert(SchemaType::Struct, [Class::Struct].into());
        m
    });

static LOGICAL_TYPE_CLASSES: LazyLock<std::collections::HashMap<&'static str, Box<[Class]>>> =
    LazyLock::new(|| {
        let mut m = std::collections::HashMap::default();
        m.insert("decimal", [Class::BigDecimal].into());
        m.insert("date", [Class::Date].into());
        m.insert("time", [Class::Date].into());
        m.insert("timestamp", [Class::Date].into());
        m
    });

pub fn build_java_class_schema_types() -> std::collections::HashMap<Class, SchemaType> {
    let mut m = std::collections::HashMap::default();
    for (&t, classes) in SCHEMA_TYPE_CLASSES.iter() {
        for &class in classes.as_ref() {
            m.insert(class, t);
        }
    }
    m
}

impl ConnectSchema {
    pub fn validate_value(&self, value: Option<&Value>) -> Result<(), String> {
        let Some(value) = value else {
            return match self.base().optional {
                true => Ok(()),
                false => Err(format!("Invalid value: null used for required field")),
            };
        };
        let found_match = self
            .expected_classes_for()
            .find(|class| is_instance(class, value))
            .is_some();
        if !found_match {
            return Err(format!("Invalid Java object for schema"));
        }
        match self {
            ConnectSchema::Struct(_) => {
                let Value::Struct(st) = value else {
                    return Err(format!("value is not struct"));
                };
                if !st.schema().equals(self) {
                    return Err(format!("Struct schemas do not match."));
                }
                st.validate()
            }
            ConnectSchema::Array(arr_schema) => {
                let Value::List(arr) = value else {
                    return Err(format!("value is not list"));
                };
                arr.iter()
                    .try_for_each(|entry| arr_schema.val_schema.validate_value(entry.as_ref()))
            }
            ConnectSchema::Map(map_schema) => {
                let Value::Map(map) = value else {
                    return Err(format!("value is not map"));
                };
                map.iter().try_for_each(|(k, v)| {
                    map_schema
                        .val_schema
                        .validate_value(Some(v))
                        .and_then(|()| map_schema.key_schema.validate_value(Some(k)))
                })
            }
            ConnectSchema::Primitive(_) => Ok(()),
        }
    }

    fn expected_classes_for(&self) -> impl Iterator<Item = Class> {
        self.base()
            .name
            .as_ref()
            .and_then(|name| LOGICAL_TYPE_CLASSES.get(name.as_ref()))
            .or_else(|| SCHEMA_TYPE_CLASSES.get(&self.base().type_))
            .map(std::ops::Deref::deref)
            .unwrap_or_default()
            .iter()
            .copied()
    }

    pub fn schema_type(class: &Class) -> Option<SchemaType> {
        build_java_class_schema_types().get(class).copied()
        // todo!: class may be a subclass of MapEnrtyKey
    }
}

impl std::fmt::Display for ConnectSchema {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.base().name {
            Some(name) => write!(f, "Schema{{{}:{}}}", name, self.base().type_),
            None => write!(f, "Schema{{{}}}", self.base().type_),
        }
    }
}
