// Copyright 2026 RisingWave Labs
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

//! Utilities for reading output data files written by the V3 PK-index sink.
//!
//! The `ResolveExecutor` calls [`scan_output_pks`] to scan each compaction output file and emit
//! `(pk_row, dense_position)` pairs that get hash-exchanged to the writer actors holding the PK
//! index.

use anyhow::{Context as _, anyhow};
use futures::{TryStreamExt as _, stream};
use futures_async_stream::try_stream;
use iceberg::arrow::ArrowReaderBuilder;
use iceberg::io::FileIO;
use iceberg::scan::FileScanTask;
use iceberg::spec::{DataContentType, DataFile, SchemaRef};
use risingwave_common::array::arrow::IcebergArrowConvert;
use risingwave_common::id::SinkId;
use risingwave_common::row::{OwnedRow, Row as _};

use crate::executor::{StreamExecutorError, StreamExecutorResult};

/// Resolve the sink's `downstream_pk` column names to Iceberg field-ids, preserving the
/// order of `pk_column_names` (the writer's index-key column order).  Every PK-pack site
/// must use this same order so the bytes match.  Bails if no PK names were supplied or
/// any name is absent from the schema — a missing name means the table schema and the
/// sink's PK have drifted, which would cause wrong bytes to be compared against the index.
pub(super) fn pk_field_ids_by_name(
    schema: &iceberg::spec::Schema,
    pk_column_names: &[String],
    sink_id: SinkId,
) -> StreamExecutorResult<Vec<i32>> {
    if pk_column_names.is_empty() {
        return Err(anyhow!(
            "iceberg v3 output scan: sink {} has no downstream PK columns to resolve",
            sink_id
        )
        .into());
    }
    let mut pk_field_ids = Vec::with_capacity(pk_column_names.len());
    for name in pk_column_names {
        let field_id = schema.field_id_by_name(name).with_context(|| {
            format!(
                "iceberg v3 output scan: PK column {:?} of sink {} not found in table schema",
                name, sink_id
            )
        })?;
        pk_field_ids.push(field_id);
    }
    Ok(pk_field_ids)
}

/// Scan one output data file projected to the PK columns; yield `(pk_row, dense_position)`.
///
/// Concurrency 1 keeps row batches in file order so positions are dense and 0-based,
/// matching what the writer originally assigned.  Delete files are intentionally absent
/// from the `FileScanTask`: we want every physical row (the resolver classifies
/// them as live/deleted after position-set arithmetic on the DV diff).
///
/// The returned stream is `'static` so it can be boxed and shipped across `await` points
/// without lifetime constraints on the caller's stack frame.
#[try_stream(ok = (OwnedRow, i64), error = StreamExecutorError)]
pub(super) async fn scan_output_pks(
    file_io: FileIO,
    schema: SchemaRef,
    output_file: DataFile,
    pk_field_ids: Vec<i32>,
) {
    let task = FileScanTask {
        file_size_in_bytes: output_file.file_size_in_bytes(),
        // start=0 / length=0 is the iceberg-rs convention for "whole file".
        start: 0,
        length: 0,
        record_count: Some(output_file.record_count()),
        data_file_path: output_file.file_path().to_owned(),
        referenced_data_file: None,
        data_file_content: DataContentType::Data,
        data_file_format: output_file.file_format(),
        schema: schema.clone(),
        project_field_ids: pk_field_ids,
        predicate: None,
        // No deletes: we need every physical row so positions stay dense and 0-based.
        deletes: vec![],
        sequence_number: 0,
        equality_ids: None,
        partition: None,
        partition_spec: None,
        name_mapping: None,
        case_sensitive: true,
    };

    // Concurrency 1 guarantees that the reader returns batches strictly in file order,
    // which is the only way the running position counter below stays correct.
    let reader = ArrowReaderBuilder::new(file_io)
        .with_data_file_concurrency_limit(1)
        .build();
    let batch_stream = reader
        .read(Box::pin(stream::iter(vec![Ok(task)])))
        .context("open output file for PK scan")?;

    // `pos` is a dense 0-based counter across ALL batches in the file.  It increments
    // once per physical row, so the caller can use it as the stable row address that the
    // writer originally stored in the PK index.
    let mut pos: i64 = 0;

    let mut batch_stream = batch_stream;
    while let Some(batch) = batch_stream
        .try_next()
        .await
        .map_err(|e| anyhow!(e).context("read record batch during output PK scan"))?
    {
        let chunk = IcebergArrowConvert
            .chunk_from_record_batch(&batch)
            .map_err(|e| anyhow!(e).context("arrow batch -> chunk in output PK scan"))?;

        // The chunk's columns are exactly the projected PK columns in `pk_field_ids` order
        // (iceberg-rs applies the column projection before returning the batch), so the
        // whole row IS the PK row — no further column selection needed.
        for row in chunk.rows() {
            let pk_row = row.to_owned_row();
            yield (pk_row, pos);
            pos += 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
    use risingwave_common::id::SinkId;

    use super::*;

    fn test_schema() -> Schema {
        // Field ids deliberately out of order vs. declaration to prove the function preserves the
        // caller's `pk_column_names` order, not the schema's field declaration order.
        Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![
                NestedField::required(7, "id", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::optional(3, "name", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::optional(9, "_rw_extra_pk", Type::Primitive(PrimitiveType::Binary))
                    .into(),
            ])
            .build()
            .unwrap()
    }

    #[test]
    fn test_pk_field_ids_by_name_preserves_order() {
        let schema = test_schema();
        let sink_id = SinkId::new(1);

        // Order follows `pk_column_names`, NOT the schema's field declaration order.
        let names = vec!["_rw_extra_pk".to_owned(), "id".to_owned()];
        let ids = pk_field_ids_by_name(&schema, &names, sink_id).unwrap();
        assert_eq!(ids, vec![9, 7]);

        let names = vec!["id".to_owned(), "name".to_owned()];
        let ids = pk_field_ids_by_name(&schema, &names, sink_id).unwrap();
        assert_eq!(ids, vec![7, 3]);
    }

    #[test]
    fn test_pk_field_ids_by_name_errors_on_unknown() {
        let schema = test_schema();
        let sink_id = SinkId::new(1);

        let names = vec!["id".to_owned(), "nonexistent".to_owned()];
        let err = pk_field_ids_by_name(&schema, &names, sink_id).unwrap_err();
        assert!(
            err.to_string().contains("nonexistent"),
            "error should name the missing column: {err}"
        );
    }

    #[test]
    fn test_pk_field_ids_by_name_errors_on_empty() {
        let schema = test_schema();
        let sink_id = SinkId::new(1);
        assert!(pk_field_ids_by_name(&schema, &[], sink_id).is_err());
    }
}
