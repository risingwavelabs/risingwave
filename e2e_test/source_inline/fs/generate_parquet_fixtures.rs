#!/usr/bin/env -S cargo -Zscript
---cargo
[package]
edition = "2024"

[dependencies]
arrow-array = "=58.1.0"
arrow-schema = "=58.1.0"
parquet = "=58.1.0"
---

use std::env;
use std::error::Error;
use std::fs::{File, create_dir_all};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow_array::{
    ArrayRef, Int16Array, Int32Array, Int64Array, RecordBatch, StringArray, StructArray,
};
use arrow_schema::{DataType, Field, Fields, Schema};
use parquet::arrow::ArrowWriter;

const DEFAULT_OUTPUT_ROOT: &str = "e2e_test/source_inline/fs/data";

/// Generates both committed Parquet regression fixtures.
fn main() -> Result<(), Box<dyn Error>> {
    let mut args = env::args_os().skip(1);
    let output_root = args
        .next()
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(DEFAULT_OUTPUT_ROOT));
    if args.next().is_some() {
        return Err("usage: generate_parquet_fixtures.rs [output-root]".into());
    }

    write_duplicate_field_names(&output_root)?;
    write_nested_smallint(&output_root)?;
    Ok(())
}

/// Writes a fixture containing two nested fields with the same name and different types.
fn write_duplicate_field_names(output_root: &Path) -> Result<(), Box<dyn Error>> {
    let struct_fields = Fields::from(vec![
        Arc::new(Field::new("v", DataType::Int64, true)),
        Arc::new(Field::new("v", DataType::Utf8, true)),
    ]);
    let struct_array = StructArray::new(
        struct_fields.clone(),
        vec![
            Arc::new(Int64Array::from(vec![42, 7])) as ArrayRef,
            Arc::new(StringArray::from(vec!["dup", "x"])) as ArrayRef,
        ],
        None,
    );
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, true),
        Field::new("s", DataType::Struct(struct_fields), true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef,
            Arc::new(struct_array) as ArrayRef,
        ],
    )?;

    write_batch(
        &output_root.join("duplicate_field_names/data.parquet"),
        &batch,
    )
}

/// Writes a fixture containing a nested field physically stored as Arrow `Int16`.
fn write_nested_smallint(output_root: &Path) -> Result<(), Box<dyn Error>> {
    let struct_fields = Fields::from(vec![Arc::new(Field::new("a", DataType::Int16, true))]);
    let struct_array = StructArray::new(
        struct_fields.clone(),
        vec![Arc::new(Int16Array::from(vec![7, -3])) as ArrayRef],
        None,
    );
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, true),
        Field::new("s", DataType::Struct(struct_fields), true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef,
            Arc::new(struct_array) as ArrayRef,
        ],
    )?;

    write_batch(&output_root.join("nested_smallint/data.parquet"), &batch)
}

/// Writes a record batch with the default Parquet writer settings.
fn write_batch(path: &Path, batch: &RecordBatch) -> Result<(), Box<dyn Error>> {
    create_dir_all(path.parent().expect("fixture path has a parent directory"))?;
    let file = File::create(path)?;
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None)?;
    writer.write(batch)?;
    writer.close()?;
    println!("wrote {}", path.display());
    Ok(())
}
