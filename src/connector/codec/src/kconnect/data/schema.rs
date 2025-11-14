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

use indexmap::IndexMap;

use crate::kconnect::errors::{SchemaBuilderException, bail_schema_builder_exception};

#[derive(Debug, Clone)]
pub struct ConnectSchema(std::sync::Arc<SchemaBuilder>);

#[derive(Debug)]
pub enum SchemaBuilder {
    Primitive(PrimitiveSchema),
    Array(ArraySchema),
    Map(MapSchema),
    Struct(StructSchema),
}

// It is unclear whether ordering within `fields` and/or `parameters` shall be considered.
impl !PartialEq for ConnectSchema {}

// In the following struct definitions, we mark the first field as private
// so that the structs cannot be constructed without using the provided constructors.
//
// Other fields are public for easier get and set.
macro_rules! readonly_field {
    ($parent:ident, $name:ident, $ty:ident) => {
        impl $parent {
            pub fn $name(&self) -> &$ty {
                &self.$name
            }
        }
    };
}

#[derive(Debug)]
pub struct SchemaBase {
    type_: SchemaType,
    pub optional: bool,
    // default: Option<Value>,
    pub name: Option<Box<str>>,
    pub version: Option<i32>,
    pub doc: Option<Box<str>>,
    /// For logical types, e.g. `decimal` with `scale`.
    pub parameters: IndexMap<Box<str>, Box<str>>,
}
readonly_field!(SchemaBase, type_, SchemaType);

#[derive(Debug)]
pub struct PrimitiveSchema {
    base: SchemaBase,
}
readonly_field!(PrimitiveSchema, base, SchemaBase);

#[derive(Debug)]
pub struct ArraySchema {
    base: SchemaBase,
    pub val_schema: ConnectSchema,
}
readonly_field!(ArraySchema, base, SchemaBase);

#[derive(Debug)]
pub struct MapSchema {
    base: SchemaBase,
    pub key_schema: ConnectSchema,
    pub val_schema: ConnectSchema,
}
readonly_field!(MapSchema, base, SchemaBase);

#[derive(Debug)]
pub struct StructSchema {
    base: SchemaBase,
    pub fields: IndexMap<Box<str>, Field>,
}
readonly_field!(StructSchema, base, SchemaBase);

#[derive(Debug)]
pub struct Field {
    index: usize,
    pub name: Box<str>,
    pub schema: ConnectSchema,
}
readonly_field!(Field, index, usize);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, strum::IntoStaticStr)]
#[strum(serialize_all = "lowercase")]
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
    /// This is `Schema.Type.getName()` in Java.
    ///
    /// Note it is different from `Schema.Type.name()` for every Java enum.
    pub fn get_name(&self) -> &'static str {
        self.into()
    }

    pub fn is_primitive(&self) -> bool {
        use SchemaType::*;
        match self {
            Int8 | Int16 | Int32 | Int64 | Float32 | Float64 | Boolean | String | Bytes => true,
            Array | Map | Struct => false,
        }
    }
}

impl SchemaBase {
    fn new(type_: SchemaType) -> Self {
        Self {
            type_,
            optional: false,
            name: None,
            version: None,
            doc: None,
            parameters: IndexMap::new(),
        }
    }
}

macro_rules! primitive_constructor {
    ($fn:ident, $ty:ident) => {
        pub fn $fn() -> Self {
            Self::Primitive(PrimitiveSchema {
                base: SchemaBase::new(SchemaType::$ty),
            })
        }
    };
}

impl SchemaBuilder {
    primitive_constructor!(int8, Int8);

    primitive_constructor!(int16, Int16);

    primitive_constructor!(int32, Int32);

    primitive_constructor!(int64, Int64);

    primitive_constructor!(float32, Float32);

    primitive_constructor!(float64, Float64);

    primitive_constructor!(bool, Boolean);

    primitive_constructor!(string, String);

    primitive_constructor!(bytes, Bytes);

    pub fn array(val_schema: ConnectSchema) -> Self {
        Self::Array(ArraySchema {
            base: SchemaBase::new(SchemaType::Array),
            val_schema,
        })
    }

    pub fn map(key_schema: ConnectSchema, val_schema: ConnectSchema) -> Self {
        Self::Map(MapSchema {
            base: SchemaBase::new(SchemaType::Map),
            key_schema,
            val_schema,
        })
    }

    pub fn struct_builder() -> StructSchema {
        StructSchema {
            base: SchemaBase::new(SchemaType::Struct),
            fields: IndexMap::new(),
        }
    }
}

impl StructSchema {
    pub fn add_field(
        &mut self,
        name: impl Into<Box<str>>,
        schema: ConnectSchema,
    ) -> Result<&mut Self, SchemaBuilderException> {
        let index = self.fields.len();
        let name = name.into();
        if name.is_empty() {
            bail_schema_builder_exception!("field name cannot be empty");
        }
        if self.fields.contains_key(&name) {
            bail_schema_builder_exception!("field name duplication {name}");
        }
        let field = Field {
            name: name.clone(),
            index,
            schema,
        };
        self.fields.insert(name, field);
        Ok(self)
    }

    pub fn build(self) -> SchemaBuilder {
        SchemaBuilder::Struct(self)
    }
}

impl ConnectSchema {
    pub fn base(&self) -> &SchemaBase {
        match self.0.as_ref() {
            SchemaBuilder::Primitive(s) => s.base(),
            SchemaBuilder::Array(s) => s.base(),
            SchemaBuilder::Map(s) => s.base(),
            SchemaBuilder::Struct(s) => s.base(),
        }
    }
}

impl SchemaBuilder {
    pub fn base_mut(&mut self) -> &mut SchemaBase {
        match self {
            Self::Primitive(s) => &mut s.base,
            Self::Array(s) => &mut s.base,
            Self::Map(s) => &mut s.base,
            Self::Struct(s) => &mut s.base,
        }
    }

    pub fn build(self) -> ConnectSchema {
        ConnectSchema(std::sync::Arc::new(self))
    }
}
