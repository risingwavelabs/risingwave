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

//  name        data type   variant     array type      owned type      ref type        primitive?
const TYPE_MATRIX: &str = "
    boolean     Boolean     Bool        BoolArray       bool            bool            _
    int16       Int16       Int16       I16Array        i16             i16             y
    int32       Int32       Int32       I32Array        i32             i32             y
    int64       Int64       Int64       I64Array        i64             i64             y
    int256      Int256      Int256      Int256Array     Int256          Int256Ref<'_>   _
    float32     Float32     Float32     F32Array        F32             F32             y
    float64     Float64     Float64     F64Array        F64             F64             y
    decimal     Decimal     Decimal     DecimalArray    Decimal         Decimal         y
    serial      Serial      Serial      SerialArray     Serial          Serial          y
    date        Date        Date        DateArray       Date            Date            y
    time        Time        Time        TimeArray       Time            Time            y
    timestamp   Timestamp   Timestamp   TimestampArray  Timestamp       Timestamp       y
    timestamptz Timestamptz Int64       I64Array        i64             i64             y
    interval    Interval    Interval    IntervalArray   Interval        Interval        y
    varchar     Varchar     Utf8        Utf8Array       Box<str>        &str            _
    bytea       Bytea       Bytea       BytesArray      Box<[u8]>       &[u8]           _
    jsonb       Jsonb       Jsonb       JsonbArray      JsonbVal        JsonbRef<'_>    _
    list        List        List        ListArray       ListValue       ListRef<'_>     _
    struct      Struct      Struct      StructArray     StructValue     StructRef<'_>   _
";

/// Maps a data type to its corresponding data type name.
pub fn data_type(ty: &str) -> &str {
    // XXX:
    // For functions that contain `any` type, there are special handlings in the frontend,
    // and the signature won't be accessed. So we simply return a placeholder here.
    if ty == "any" {
        return "Int32";
    } else if ty.ends_with("[]") {
        return "List";
    }
    lookup_matrix(ty, 1)
}

/// Maps a data type to its corresponding variant name.
pub fn variant(ty: &str) -> &str {
    lookup_matrix(ty, 2)
}

/// Maps a data type to its corresponding array type name.
pub fn array_type(ty: &str) -> &str {
    if ty == "any" {
        return "ArrayImpl";
    } else if ty.ends_with("[]") {
        return "ListArray";
    }
    lookup_matrix(ty, 3)
}

/// Maps a data type to its corresponding `Scalar` type name.
pub fn owned_type(ty: &str) -> &str {
    lookup_matrix(ty, 4)
}

/// Maps a data type to its corresponding `ScalarRef` type name.
pub fn ref_type(ty: &str) -> &str {
    lookup_matrix(ty, 5)
}

/// Checks if a data type is primitive.
pub fn is_primitive(ty: &str) -> bool {
    lookup_matrix(ty, 6) == "y"
}

fn lookup_matrix(ty: &str, idx: usize) -> &str {
    let s = TYPE_MATRIX.trim().lines().find_map(|line| {
        let mut parts = line.split_whitespace();
        if parts.next() == Some(ty) {
            Some(parts.nth(idx - 1).unwrap())
        } else {
            None
        }
    });
    s.unwrap_or_else(|| panic!("unknown type: {}", ty))
}

/// Expands a type wildcard string into a list of concrete types.
pub fn expand_type_wildcard(ty: &str) -> Vec<&str> {
    match ty {
        "*" => TYPE_MATRIX
            .trim()
            .lines()
            .map(|l| l.split_whitespace().next().unwrap())
            .collect(),
        "*int" => vec!["int16", "int32", "int64"],
        "*numeric" => vec!["decimal"],
        "*float" => vec!["float32", "float64"],
        _ => vec![ty],
    }
}

/// Computes the minimal compatible type between a pair of data types.
///
/// This function is used to determine the `auto` type.
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

        ("int32", "int16") => "int32",
        ("int32", "int32") => "int32",
        ("int32", "int64") => "int64",

        ("int64", "int16") => "int64",
        ("int64", "int32") => "int64",
        ("int64", "int64") => "int64",

        ("int16", "int256") => "int256",
        ("int32", "int256") => "int256",
        ("int64", "int256") => "int256",
        ("int256", "int16") => "int256",
        ("int256", "int32") => "int256",
        ("int256", "int64") => "int256",
        ("int256", "float64") => "float64",
        ("float64", "int256") => "float64",

        ("float32", "float32") => "float32",
        ("float32", "float64") => "float64",

        ("float64", "float32") => "float64",
        ("float64", "float64") => "float64",

        ("decimal", "decimal") => "decimal",

        ("date", "timestamp") => "timestamp",
        ("timestamp", "date") => "timestamp",
        ("time", "interval") => "interval",
        ("interval", "time") => "interval",

        (a, b) => panic!("unknown minimal compatible type for {a:?} and {b:?}"),
    }
}
