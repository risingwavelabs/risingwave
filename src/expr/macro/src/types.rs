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

//! This module provides utility functions for SQL data type conversion and manipulation.

/// Expands a type wildcard string into a list of concrete types.
pub fn expand_type_wildcard(ty: &str) -> Vec<&str> {
    match ty {
        "*" => vec![
            "boolean",
            "int16",
            "int32",
            "int64",
            "float32",
            "float64",
            "decimal",
            "serial",
            "date",
            "time",
            "timestamp",
            "timestamptz",
            "interval",
            "varchar",
            "bytea",
            "jsonb",
            "struct",
            "list",
        ],
        "*int" => vec!["int16", "int32", "int64"],
        "*number" => vec!["int16", "int32", "int64", "float32", "float64", "decimal"],
        _ => vec![ty],
    }
}

/// Maps a data type to its corresponding type name.
pub fn to_data_type_name(ty: &str) -> Option<&str> {
    Some(match ty {
        "boolean" => "Boolean",
        "int16" => "Int16",
        "int32" => "Int32",
        "int64" => "Int64",
        "float32" => "Float32",
        "float64" => "Float64",
        "decimal" => "Decimal",
        "serial" => "Serial",
        "date" => "Date",
        "time" => "Time",
        "timestamp" => "Timestamp",
        "timestamptz" => "Timestamptz",
        "interval" => "Interval",
        "varchar" => "Varchar",
        "bytea" => "Bytea",
        "jsonb" => "Jsonb",
        "struct" => "Struct",
        "list" => "List",
        _ => return None,
    })
}

/// Computes the minimal compatible type between a pair of data types.
pub fn min_compatible_type(types: &[impl AsRef<str>]) -> &str {
    if types.len() == 1 {
        return types[0].as_ref();
    }
    assert_eq!(types.len(), 2);
    match (types[0].as_ref(), types[1].as_ref()) {
        (a, b) if a == b => a,

        ("int16", "int16") => "int16",
        ("int16", "int32") => "int32",
        ("int16", "int64") => "int64",
        ("int16", "float32") => "float64",
        ("int16", "float64") => "float64",
        ("int16", "decimal") => "decimal",

        ("int32", "int16") => "int32",
        ("int32", "int32") => "int32",
        ("int32", "int64") => "int64",
        ("int32", "float32") => "float64",
        ("int32", "float64") => "float64",
        ("int32", "decimal") => "decimal",

        ("int64", "int16") => "int64",
        ("int64", "int32") => "int64",
        ("int64", "int64") => "int64",
        ("int64", "float32") => "float64",
        ("int64", "float64") => "float64",
        ("int64", "decimal") => "decimal",

        ("float32", "int16") => "float64",
        ("float32", "int32") => "float64",
        ("float32", "int64") => "float64",
        ("float32", "float32") => "float32",
        ("float32", "float64") => "float64",
        ("float32", "decimal") => "float64",

        ("float64", "int16") => "float64",
        ("float64", "int32") => "float64",
        ("float64", "int64") => "float64",
        ("float64", "float32") => "float64",
        ("float64", "float64") => "float64",
        ("float64", "decimal") => "float64",

        ("decimal", "int16") => "decimal",
        ("decimal", "int32") => "decimal",
        ("decimal", "int64") => "decimal",
        ("decimal", "float32") => "float64",
        ("decimal", "float64") => "float64",
        ("decimal", "decimal") => "decimal",

        ("date", "timestamp") => "timestamp",
        ("timestamp", "date") => "timestamp",
        ("time", "interval") => "interval",
        ("interval", "time") => "interval",

        (a, b) => panic!("unknown minimal compatible type for {a:?} and {b:?}"),
    }
}

/// Maps a data type to its corresponding array type name.
pub fn to_array_type(ty: &str) -> &str {
    match ty {
        "boolean" => "BoolArray",
        "int16" => "I16Array",
        "int32" => "I32Array",
        "int64" => "I64Array",
        "float32" => "F32Array",
        "float64" => "F64Array",
        "decimal" => "DecimalArray",
        "serial" => "SerialArray",
        "date" => "NaiveDateArray",
        "time" => "NaiveTimeArray",
        "timestamp" => "NaiveDateTimeArray",
        "timestamptz" => "I64Array",
        "interval" => "IntervalArray",
        "varchar" => "Utf8Array",
        "bytea" => "BytesArray",
        "jsonb" => "JsonbArray",
        "struct" => "StructArray",
        "list" => "ListArray",
        _ => panic!("unknown type: {ty:?}"),
    }
}

/// Maps a data type to its corresponding `ScalarRef` type name.
pub fn to_data_type(ty: &str) -> &str {
    match ty {
        "boolean" => "bool",
        "int16" => "i16",
        "int32" => "i32",
        "int64" => "i64",
        "float32" => "OrderedF32",
        "float64" => "OrderedF64",
        "decimal" => "Decimal",
        "serial" => "Serial",
        "date" => "NaiveDateWrapper",
        "time" => "NaiveTimeWrapper",
        "timestamp" => "NaiveDateTimeWrapper",
        "timestamptz" => "i64",
        "interval" => "IntervalUnit",
        "varchar" => "&str",
        "bytea" => "&[u8]",
        "jsonb" => "JsonbRef<'_>",
        "struct" => "StructRef<'_>",
        "list" => "ListRef<'_>",
        _ => panic!("unknown type: {ty:?}"),
    }
}

/// Checks if a data type is primitive.
pub fn is_primitive(ty: &str) -> bool {
    match ty {
        "int16" | "int32" | "int64" | "float32" | "float64" | "decimal" | "date" | "time"
        | "timestamp" | "timestamptz" | "interval" | "serial" => true,
        "boolean" | "varchar" | "bytea" | "jsonb" | "struct" | "list" => false,
        _ => panic!("unknown type: {ty:?}"),
    }
}
