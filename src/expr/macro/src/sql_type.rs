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

pub fn to_data_type_name(ty: &str) -> Option<&str> {
    Some(match ty {
        "boolean" => "Boolean",
        "int16" => "Int16",
        "int32" => "Int32",
        "int64" => "Int64",
        "float32" => "Float32",
        "float64" => "Float64",
        "decimal" => "Decimal",
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

pub fn to_array_type(ty: &str) -> &str {
    match ty {
        "boolean" => "BoolArray",
        "int16" => "I16Array",
        "int32" => "I32Array",
        "int64" => "I64Array",
        "float32" => "F32Array",
        "float64" => "F64Array",
        "decimal" => "DecimalArray",
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

pub fn to_data_type(ty: &str) -> &str {
    match ty {
        "boolean" => "bool",
        "int16" => "i16",
        "int32" => "i32",
        "int64" => "i64",
        "float32" => "OrderedF32",
        "float64" => "OrderedF64",
        "decimal" => "Decimal",
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

pub fn is_primitive(ty: &str) -> bool {
    match ty {
        "int16" | "int32" | "int64" | "float32" | "float64" | "decimal" | "date" | "time"
        | "timestamp" | "timestamptz" | "interval" => true,
        "boolean" | "varchar" | "bytea" | "jsonb" | "struct" | "list" => false,
        _ => panic!("unknown type: {ty:?}"),
    }
}
