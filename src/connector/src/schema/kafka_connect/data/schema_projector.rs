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

use std::sync::LazyLock;

use super::schema::{ConnectSchema, SchemaType, Value};
use super::struct_::Struct;
use crate::schema::kafka_connect::error::SchemaProjectorException;

static PROMOTABLE: LazyLock<std::collections::HashSet<(SchemaType, SchemaType)>> =
    LazyLock::new(|| {
        let promotable_types = [
            SchemaType::Int8,
            SchemaType::Int16,
            SchemaType::Int32,
            SchemaType::Int64,
            SchemaType::Float32,
            SchemaType::Float64,
        ];

        let mut m = std::collections::HashSet::default();

        for (i, &src) in promotable_types.iter().enumerate() {
            for &dst in &promotable_types[i..] {
                m.insert((src, dst));
            }
        }

        m
    });

pub fn project(
    source: &ConnectSchema,
    record: Option<&Value>,
    target: &ConnectSchema,
) -> Result<Option<Value>, SchemaProjectorException> {
    check_maybe_compatible(source, target)?;
    if source.base().optional && !target.base().optional {
        if let Some(def) = &target.base().default {
            if let Some(record) = record {
                project_required_schema(source, record, target).map(Some)
            } else {
                Ok(Some(def.clone()))
            }
        } else {
            Err(SchemaProjectorException::new("Writer schema is optional, however, target schema does not provide a default value."))
        }
    } else {
        if let Some(record) = record {
            project_required_schema(source, record, target).map(Some)
        } else {
            Ok(None)
        }
    }
}

fn project_required_schema(
    source: &ConnectSchema,
    record: &Value,
    target: &ConnectSchema,
) -> Result<Value, SchemaProjectorException> {
    match target.base().type_ {
        SchemaType::Int8
        | SchemaType::Int16
        | SchemaType::Int32
        | SchemaType::Int64
        | SchemaType::Float32
        | SchemaType::Float64
        | SchemaType::Boolean
        | SchemaType::String
        | SchemaType::Bytes => project_primitive(source, record, target),
        SchemaType::Array => project_array(source, record, target).map(Value::List),
        SchemaType::Map => project_map(source, record, target).map(Value::Map),
        SchemaType::Struct => project_struct(source, record, target).map(Value::Struct),
    }
}

fn project_struct(
    source: &ConnectSchema,
    record: &Value,
    target: &ConnectSchema,
) -> Result<Struct, SchemaProjectorException> {
    let Value::Struct(source_struct) = record else {
        unreachable!()
    };
    let ConnectSchema::Struct(source) = source else {
        unreachable!()
    };
    let ConnectSchema::Struct(target) = target else {
        unreachable!()
    };

    let mut target_struct = Struct::new(target.clone());
    for target_field in &target.fields {
        let field_name = &target_field.name;
        if let Some(source_field) = source.field(field_name) {
            let source_field_val = source_struct
                .get_by_name(field_name)
                .map_err(|_e| todo!())?;
            let target_field_val =
                project(&source_field.schema, source_field_val, &target_field.schema).map_err(
                    |_e| SchemaProjectorException::new("Error projecting {field_name} {e}"),
                )?;
            target_struct
                .put_by_name(field_name, target_field_val)
                .map_err(|_e| todo!())?;
        } else if target_field.schema.base().optional {
            // Ignore missing field
        } else if let Some(def) = &target_field.schema.base().default {
            target_struct
                .put_by_name(field_name, Some(def.clone()))
                .map_err(|_e| todo!())?;
        } else {
            return Err(SchemaProjectorException::new(
                "Required field `{field_name}` is missing from source schema: todo",
            ));
        }
    }
    Ok(target_struct)
}

fn project_array(
    source: &ConnectSchema,
    record: &Value,
    target: &ConnectSchema,
) -> Result<Vec<Option<Value>>, SchemaProjectorException> {
    let Value::List(arr) = record else {
        unreachable!()
    };
    let ConnectSchema::Array(source) = source else {
        unreachable!()
    };
    let ConnectSchema::Array(target) = target else {
        unreachable!()
    };

    arr.iter()
        .map(|entry| project(&source.val_schema, entry.as_ref(), &target.val_schema))
        .collect()
}

fn project_map(
    source: &ConnectSchema,
    record: &Value,
    target: &ConnectSchema,
) -> Result<std::collections::HashMap<Value, Value>, SchemaProjectorException> {
    let Value::Map(map) = record else {
        unreachable!()
    };
    let ConnectSchema::Map(source) = source else {
        unreachable!()
    };
    let ConnectSchema::Map(target) = target else {
        unreachable!()
    };

    map.iter()
        .map(|(key, val)| {
            Ok((
                project(&source.key_schema, Some(key), &target.key_schema)?.unwrap(),
                project(&source.val_schema, Some(val), &target.val_schema)?.unwrap(),
            ))
        })
        .collect()
}

fn project_primitive(
    source: &ConnectSchema,
    record: &Value,
    target: &ConnectSchema,
) -> Result<Value, SchemaProjectorException> {
    assert!(source.base().type_.is_primitive());
    assert!(target.base().type_.is_primitive());
    if is_promotable(source.base().type_, target.base().type_) {
        match target.base().type_ {
            SchemaType::Int8 => todo!(),
            SchemaType::Int16 => todo!(),
            SchemaType::Int32 => todo!(),
            SchemaType::Int64 => todo!(),
            SchemaType::Float32 => todo!(),
            SchemaType::Float64 => todo!(),
            _ => Err(SchemaProjectorException::new("Not promotable type.")),
        }
    } else {
        Ok(record.clone())
    }
}

fn check_maybe_compatible(
    source: &ConnectSchema,
    target: &ConnectSchema,
) -> Result<(), SchemaProjectorException> {
    if source.base().type_ != target.base().type_
        && !is_promotable(source.base().type_, target.base().type_)
    {
        return Err(SchemaProjectorException::new(format!(
            "Schema type mismatch."
        )));
    } else if source.base().name != target.base().name {
        return Err(SchemaProjectorException::new(format!(
            "Schema name mismatch."
        )));
    } else if source.base().parameters != target.base().parameters {
        return Err(SchemaProjectorException::new(format!(
            "Schema parameters not equal."
        )));
    }
    Ok(())
}

fn is_promotable(source: SchemaType, target: SchemaType) -> bool {
    PROMOTABLE.contains(&(source, target))
}
