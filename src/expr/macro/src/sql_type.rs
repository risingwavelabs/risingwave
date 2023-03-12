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

pub fn min_compatible_type(types: &[String]) -> &str {
    if types.len() == 1 {
        return types[0].as_str();
    }
    assert_eq!(types.len(), 2);
    match (types[0].as_str(), types[1].as_str()) {
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

        (a, b) => panic!("unknown minimal compatible type for {a:?} and {b:?}"),
    }
}
