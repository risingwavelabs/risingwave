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

//! Resolver for concurrent compaction of an Iceberg V3 (merge-on-read, `enable_pk_index`) sink.
//!
//! BergLoom rewrites data files of a V3 table WITHOUT committing, reading the rows live at some
//! snapshot `R` (`read_snapshot_id`) and reporting `{output_files, input_file_paths, R}` back to
//! meta. While that rewrite was in flight the live writers may have appended new deletion-vector
//! (DV) positions to the input data files. Those `(R, N]` deletes are absent from the output files
//! (which only know the world as of `R`), so before the meta coordinator overwrites the inputs with
//! the outputs we must re-apply them as fresh DVs on the output rows. `N` is the table's current
//! snapshot; the caller holds the per-sink coordinator mutex, so `N` is fixed for the duration of
//! this call.
//!
//! The resolver is structured so the IO-free core (PK packing, delete-diff set math, and the
//! hit/miss classification) lives in small free functions that are unit-tested below. The
//! Parquet/Puffin IO paths mirror the proven patterns in
//! [`crate::stream::executor::iceberg_with_pk_index::dv_handler_impl`] (DV read/write) and the
//! connector's iceberg parquet reader (column-projected scan).
//!
//! PK source: the V3 `enable_pk_index` sink NEVER writes Iceberg identifier-field-ids onto the
//! table schema, so the PK is NOT the Iceberg identifier set. Instead the PK is the sink's
//! `downstream_pk` columns (plain user columns, or a visible synthetic `_rw_extra_pk` Bytea column
//! when the upstream stream key has hidden columns), matched to the table schema BY NAME. The
//! caller threads those names in (in `downstream_pk` order, which is the writer's index-key column
//! order) and we resolve them to Iceberg field-ids here.
//!
//! Invariants (over/under-DV is silent data loss, so honor them exactly):
//! - Output files contain exactly the rows live at `R`; deletes at-or-before `R` are already absent.
//! - V3 input *data* files are stable across `(R, N]` (writers only APPEND data and ADD DV files,
//!   never rewrite data files), so the set of input data files at `R` equals that at `N`.
//! - Only the input files' DVs grow across the window; the dead PKs are exactly the positions newly
//!   deleted in `(R, N]` = `dv_n \ dv_r` per input data file.
//! - `input_files_to_delete` must include EVERY DV (as seen at `N`) referencing an input data file,
//!   not just `R`'s, or those DVs dangle after the input data files are removed by the overwrite.

// The coordinator consumes `output_dv_files` and `input_files_to_delete`, but `RemapRow`'s fields
// are written for a future milestone (M3 PK-index repair) and not yet read — the caller only takes
// `remap_rows.len()`. Silence dead-code warnings here rather than scattering per-item attributes.
#![allow(dead_code)]

use std::collections::{HashMap, HashSet};

use anyhow::{Context, Result, anyhow, bail};
use bytes::Bytes;
use futures::{TryStreamExt, stream};
use iceberg::Catalog;
use iceberg::arrow::ArrowReaderBuilder;
use iceberg::delete_vector::DeleteVector;
use iceberg::io::FileIO;
use iceberg::puffin::{CompressionCodec, PuffinReader, PuffinWriter};
use iceberg::scan::FileScanTask;
use iceberg::spec::{
    DataContentType, DataFile, DataFileBuilder, DataFileFormat, ManifestContentType, ManifestList,
    PartitionKey, Schema, SchemaRef, SerializedDataFile,
};
use iceberg::table::Table;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator, FileNameGenerator, LocationGenerator,
};
use risingwave_common::array::arrow::IcebergArrowConvert;
use risingwave_common::row::Row;
use risingwave_common::util::memcmp_encoding;
use risingwave_common::util::sort_util::OrderType;
use risingwave_connector::sink::catalog::SinkId;
use uuid::Uuid;

/// Format version byte prepended to every packed PK. Mirrors `_rw_pack_pk`'s `FORMAT_VERSION`
/// (`src/expr/impl/src/scalar/rw_pack_pk.rs`) so the bytes are encoded the same way. The resolver
/// only needs self-consistency between the input-read side and the output-scan side, but we mirror
/// the original encoding exactly to keep the two definitions from drifting.
const PK_FORMAT_VERSION: u8 = 0x01;

/// Puffin blob property for deletion vector cardinality.
const DELETION_VECTOR_PROPERTY_CARDINALITY: &str = "cardinality";
/// Puffin blob property for referenced data file path.
const DELETION_VECTOR_PROPERTY_REFERENCED_DATA_FILE: &str = "referenced-data-file";

/// Output of resolving one compaction against the live table.
pub(super) struct CompactionResolution {
    /// New Puffin deletion-vector files for "hit" output positions (rows deleted in `(R, N]`).
    pub output_dv_files: Vec<SerializedDataFile>,
    /// Input data + DV files (as seen at `N`) to remove in the overwrite. MUST include the `(R, N]`
    /// input DVs, not just `R`'s, or they dangle after the input data files are removed.
    pub input_files_to_delete: Vec<DataFile>,
    /// Survivor remap rows. Returned for a future milestone; the M2 caller ignores them.
    pub remap_rows: Vec<RemapRow>,
}

/// A surviving (non-deleted) output row, keyed by its packed PK and located by `(file, pos)`.
pub(super) struct RemapRow {
    pub packed_pk: Bytes,
    pub output_file: String,
    pub output_pos: u64,
}

/// Resolve a V3 compaction. Caller holds the per-sink coordinator mutex, so `table`'s current
/// snapshot is the commit upper bound `N`.
pub(super) async fn resolve_compaction(
    table: &Table,
    _catalog: &dyn Catalog,
    sink_id: SinkId,
    output_files: &[SerializedDataFile],
    input_file_paths: &[String],
    read_snapshot_id: i64,
    pk_column_names: &[String],
) -> Result<CompactionResolution> {
    let schema = table.metadata().current_schema();

    // Guard: only unpartitioned tables are sound here. The output-decode below stamps every output
    // file with the table's DEFAULT partition spec/type; for a partitioned table whose output files
    // were written under a non-default spec, the partition raw-literal would be decoded against the
    // wrong spec and the wrong partition stamped onto the output DV files (silent corruption).
    // Partitioned support is deferred (R4): it needs per-file `partition_spec_id` plumbed through the
    // report proto. Fire BEFORE any output-file decoding so a partitioned table never reaches it.
    if !table.metadata().default_partition_spec().is_unpartitioned() {
        bail!(
            "partitioned V3 sink compaction is not yet supported (sink {}); table has a partitioned spec",
            sink_id
        );
    }

    // Step 1: PK = the sink's `downstream_pk` columns, matched to the table schema BY NAME. The V3
    // `enable_pk_index` sink never sets Iceberg identifier-field-ids, so we cannot read them off the
    // schema; the caller supplies the PK column names in `downstream_pk` order (the writer's
    // index-key column order), which we preserve so every pack site agrees on column order.
    let pk_field_ids = pk_field_ids_by_name(schema, pk_column_names, sink_id)?;

    let snapshot_n = table
        .metadata()
        .current_snapshot()
        .with_context(|| {
            format!(
                "iceberg v3 compaction resolver: table for sink {} has no current snapshot",
                sink_id
            )
        })?
        .clone();
    let snapshot_r = table
        .metadata()
        .snapshot_by_id(read_snapshot_id)
        .with_context(|| {
            format!(
                "iceberg v3 compaction resolver: read_snapshot_id {} not found for sink {}",
                read_snapshot_id, sink_id
            )
        })?
        .clone();

    let manifest_list_n = table
        .object_cache()
        .get_manifest_list(&snapshot_n, &table.metadata_ref())
        .await
        .context("load manifest list at N")?;
    let manifest_list_r = table
        .object_cache()
        .get_manifest_list(&snapshot_r, &table.metadata_ref())
        .await
        .context("load manifest list at R")?;

    // Step 2: re-derive the input DATA files at R (stable across the window).
    let input_paths: HashSet<&str> = input_file_paths.iter().map(String::as_str).collect();
    let input_data_files = collect_input_data_files(table, &manifest_list_r, &input_paths).await?;

    // Step 3a: every DV (at N) referencing an input data file must be removed in the overwrite.
    let input_dv_files =
        collect_referencing_dv_files(table, &manifest_list_n, &input_paths).await?;

    // Step 3b: per input data file, diff its DV at R vs N and collect the PKs of the newly dead rows.
    let dvs_at_r = load_referencing_dvs_by_data_file(table, &manifest_list_r, &input_paths).await?;
    let dvs_at_n_positions = positions_by_data_file(&input_dv_files, table).await?;

    let mut dead_pks: HashSet<Bytes> = HashSet::new();
    for data_file in &input_data_files {
        let path = data_file.file_path();
        let dv_r = dvs_at_r
            .get(path)
            .map(|dv| dv.iter().collect::<HashSet<u64>>())
            .unwrap_or_default();
        let dv_n = dvs_at_n_positions.get(path).cloned().unwrap_or_default();
        let dead = diff_positions(&dv_n, &dv_r);
        if dead.is_empty() {
            continue;
        }
        collect_dead_pks(
            table,
            data_file,
            schema,
            &pk_field_ids,
            &dead,
            &mut dead_pks,
        )
        .await?;
    }

    // Step 4: scan output files, classify each row as hit (dead) or miss (survivor).
    let partition_spec_id = table.metadata().default_partition_spec_id();
    let partition_type = table.metadata().default_partition_type();

    let mut remap_rows = Vec::new();
    let mut output_dv_files = Vec::new();
    let mut file_name_generator =
        DefaultFileNameGenerator::new(Uuid::now_v7().to_string(), None, DataFileFormat::Puffin);
    let location_generator = DefaultLocationGenerator::new(table.metadata().clone())
        .context("build location generator for output DV")?;

    for serialized in output_files {
        let data_file = serialized
            .clone()
            .try_into(partition_spec_id, partition_type, schema)
            .map_err(|e| anyhow!(e).context("materialize output SerializedDataFile"))?;
        let output_path = data_file.file_path().to_owned();

        let mut hit_dv = DeleteVector::default();
        let mut output_pos: u64 = 0;
        let mut packed_iter = scan_packed_pks(table, &data_file, schema, &pk_field_ids).await?;
        while let Some(packed_pk) = packed_iter.try_next().await? {
            if classify_hit(&packed_pk, &dead_pks) {
                hit_dv.insert(output_pos);
            } else {
                // NOTE: the `packed_pk` here is the resolver's OWN self-consistent encoding
                // (`pack_pk_row`: version byte + ascending memcmp of the PK columns in
                // `pk_column_names` order). That is sufficient for compaction resolution because the
                // input-read and output-scan sides pack identically and only compare against each
                // other. It is intentionally NOT reconciled against the writer's `pk_index`
                // state-table key here: a future milestone (M3) that feeds `remap_rows` back into
                // the writer's `OrderedRowSerde` index must translate this encoding (they differ:
                // `_rw_pack_pk` has a version byte + memcmp; the index key is vnode + memcmp). Out of
                // scope for this milestone.
                remap_rows.push(RemapRow {
                    packed_pk,
                    output_file: output_path.clone(),
                    output_pos,
                });
            }
            output_pos += 1;
        }

        // Step 6: write a Puffin DV for the hit positions of this output file.
        if !hit_dv.is_empty() {
            let dv_data_file = write_output_dv_puffin_file(
                table,
                &data_file,
                hit_dv,
                &location_generator,
                &mut file_name_generator,
            )
            .await?;
            let serialized = SerializedDataFile::try_from(
                dv_data_file,
                partition_type,
                table.metadata().format_version(),
            )
            .map_err(|e| anyhow!(e).context("serialize output DV file"))?;
            output_dv_files.push(serialized);
        }
    }

    // A file path may appear in more than one manifest; dedup by path so each distinct file is
    // queued for deletion at most once (a path deleted twice would dangle in the overwrite).
    // Existing order is preserved: data files first, then DV files, first occurrence wins.
    let mut seen_paths: HashSet<String> = HashSet::new();
    let mut input_files_to_delete =
        Vec::with_capacity(input_data_files.len() + input_dv_files.len());
    for file in input_data_files
        .into_iter()
        .chain(input_dv_files.into_iter())
    {
        if seen_paths.insert(file.file_path().to_owned()) {
            input_files_to_delete.push(file);
        }
    }

    Ok(CompactionResolution {
        output_dv_files,
        input_files_to_delete,
        remap_rows,
    })
}

/// Resolve the sink's `downstream_pk` column names to Iceberg field-ids, PRESERVING the order of
/// `pk_column_names` (the writer's index-key column order). Every PK-pack site must use this same
/// order so the packed bytes match. Bails if no PK names were supplied or any name is missing from
/// the schema (a missing name means the table schema and the sink's PK have drifted — packing would
/// silently produce wrong bytes).
fn pk_field_ids_by_name(
    schema: &Schema,
    pk_column_names: &[String],
    sink_id: SinkId,
) -> Result<Vec<i32>> {
    if pk_column_names.is_empty() {
        bail!(
            "iceberg v3 compaction resolver: sink {} has no downstream PK columns to resolve",
            sink_id
        );
    }
    let mut pk_field_ids = Vec::with_capacity(pk_column_names.len());
    for name in pk_column_names {
        let field_id = schema.field_id_by_name(name).with_context(|| {
            format!(
                "iceberg v3 compaction resolver: PK column {:?} of sink {} not found in table schema",
                name, sink_id
            )
        })?;
        pk_field_ids.push(field_id);
    }
    Ok(pk_field_ids)
}

/// Walk snapshot `R`'s manifest list and collect the `DataFile`s whose path is an input path and
/// whose content is `Data`. These input data files are stable across `(R, N]`.
async fn collect_input_data_files(
    table: &Table,
    manifest_list: &ManifestList,
    input_paths: &HashSet<&str>,
) -> Result<Vec<DataFile>> {
    let mut out = Vec::new();
    for manifest_file in manifest_list.entries() {
        if manifest_file.content != ManifestContentType::Data {
            continue;
        }
        let manifest = manifest_file
            .load_manifest(table.file_io())
            .await
            .context("load data manifest at R")?;
        for entry in manifest.entries() {
            if !entry.is_alive() || entry.content_type() != DataContentType::Data {
                continue;
            }
            let data_file = entry.data_file();
            if input_paths.contains(data_file.file_path()) {
                out.push(data_file.clone());
            }
        }
    }
    Ok(out)
}

/// Collect every alive Puffin DV `DataFile` (as seen at the given manifest list) that references one
/// of the input data files.
async fn collect_referencing_dv_files(
    table: &Table,
    manifest_list: &ManifestList,
    input_paths: &HashSet<&str>,
) -> Result<Vec<DataFile>> {
    let mut out = Vec::new();
    for manifest_file in manifest_list.entries() {
        if manifest_file.content != ManifestContentType::Deletes {
            continue;
        }
        let manifest = manifest_file
            .load_manifest(table.file_io())
            .await
            .context("load delete manifest")?;
        for entry in manifest.entries() {
            if !entry.is_alive()
                || entry.content_type() != DataContentType::PositionDeletes
                || entry.file_format() != DataFileFormat::Puffin
            {
                continue;
            }
            let data_file = entry.data_file();
            let Some(referenced) = data_file.referenced_data_file() else {
                continue;
            };
            if input_paths.contains(referenced.as_str()) {
                out.push(data_file.clone());
            }
        }
    }
    Ok(out)
}

/// Load, per input data file, the merged DV at the given manifest list (snapshot `R`). Returns a map
/// from input data file path to its `DeleteVector`.
async fn load_referencing_dvs_by_data_file(
    table: &Table,
    manifest_list: &ManifestList,
    input_paths: &HashSet<&str>,
) -> Result<HashMap<String, DeleteVector>> {
    let dv_files = collect_referencing_dv_files(table, manifest_list, input_paths).await?;
    let mut out: HashMap<String, DeleteVector> = HashMap::new();
    for dv_file in &dv_files {
        let referenced = dv_file
            .referenced_data_file()
            .context("DV file missing referenced_data_file")?;
        let dv = read_dv_positions(table.file_io(), dv_file).await?;
        match out.entry(referenced) {
            std::collections::hash_map::Entry::Occupied(mut e) => *e.get_mut() |= dv,
            std::collections::hash_map::Entry::Vacant(e) => {
                e.insert(dv);
            }
        }
    }
    Ok(out)
}

/// Materialize the DV positions (at N) per input data file from already-collected DV files.
async fn positions_by_data_file(
    dv_files: &[DataFile],
    table: &Table,
) -> Result<HashMap<String, HashSet<u64>>> {
    let mut out: HashMap<String, HashSet<u64>> = HashMap::new();
    for dv_file in dv_files {
        let referenced = dv_file
            .referenced_data_file()
            .context("DV file missing referenced_data_file")?;
        let dv = read_dv_positions(table.file_io(), dv_file).await?;
        let set = out.entry(referenced).or_default();
        for pos in dv.iter() {
            set.insert(pos);
        }
    }
    Ok(out)
}

/// Read a Puffin DV blob into a `DeleteVector`. Mirrors
/// `dv_handler_impl::read_dv_positions_from_data_file`.
async fn read_dv_positions(file_io: &FileIO, dv_file: &DataFile) -> Result<DeleteVector> {
    let blob_offset = dv_file
        .content_offset()
        .context("DV file missing content_offset")?;
    let blob_length = dv_file
        .content_size_in_bytes()
        .context("DV file missing content_size_in_bytes")?;

    let input_file = file_io
        .new_input(dv_file.file_path())
        .context("open DV puffin file")?;
    let puffin_reader = PuffinReader::new(input_file);
    let file_metadata = puffin_reader
        .file_metadata()
        .await
        .context("read DV puffin metadata")?;
    let blob_metadata = file_metadata
        .blobs()
        .iter()
        .find(|blob| blob.offset() == blob_offset as u64 && blob.length() == blob_length as u64)
        .with_context(|| {
            format!(
                "DV blob not found in {} at offset={} length={}",
                dv_file.file_path(),
                blob_offset,
                blob_length
            )
        })?;
    let blob = puffin_reader
        .blob(blob_metadata)
        .await
        .context("read DV puffin blob")?;
    DeleteVector::from_puffin_blob(blob).context("decode DV puffin blob")
}

/// Read the PK columns of the dead positions of `data_file` and insert their packed PKs into `out`.
async fn collect_dead_pks(
    table: &Table,
    data_file: &DataFile,
    schema: &SchemaRef,
    pk_field_ids: &[i32],
    dead: &HashSet<u64>,
    out: &mut HashSet<Bytes>,
) -> Result<()> {
    let mut pos: u64 = 0;
    let mut packed_iter = scan_packed_pks(table, data_file, schema, pk_field_ids).await?;
    while let Some(packed_pk) = packed_iter.try_next().await? {
        if dead.contains(&pos) {
            out.insert(packed_pk);
        }
        pos += 1;
    }
    Ok(())
}

/// Scan a data file projected to the PK columns (in `pk_field_ids` order) and yield each row's
/// packed PK in file order, so the caller can pair the index with a 0-based row position.
///
/// We deliberately pass no delete files in the `FileScanTask` so every physical row is returned
/// (the dead-row info comes from the DV diff, not from filtering), keeping positions dense.
async fn scan_packed_pks(
    table: &Table,
    data_file: &DataFile,
    schema: &SchemaRef,
    pk_field_ids: &[i32],
) -> Result<futures::stream::BoxStream<'static, Result<Bytes>>> {
    let task = FileScanTask {
        file_size_in_bytes: data_file.file_size_in_bytes(),
        start: 0,
        length: 0,
        record_count: Some(data_file.record_count()),
        data_file_path: data_file.file_path().to_owned(),
        referenced_data_file: None,
        data_file_content: DataContentType::Data,
        data_file_format: data_file.file_format(),
        schema: schema.clone(),
        project_field_ids: pk_field_ids.to_vec(),
        predicate: None,
        deletes: vec![],
        sequence_number: 0,
        equality_ids: None,
        partition: None,
        partition_spec: None,
        name_mapping: None,
        case_sensitive: true,
    };

    // Concurrency 1 keeps batches in file order, so row positions stay dense and 0-based.
    let reader = ArrowReaderBuilder::new(table.file_io().clone())
        .with_data_file_concurrency_limit(1)
        .build();
    let batch_stream = reader
        .read(Box::pin(stream::iter(vec![Ok(task)])))
        .context("read data file for PK scan")?;

    let stream = batch_stream
        .map_err(|e| anyhow!(e).context("read record batch during PK scan"))
        .and_then(|batch| async move {
            let chunk = IcebergArrowConvert
                .chunk_from_record_batch(&batch)
                .map_err(|e| anyhow!(e).context("arrow batch -> chunk"))?;
            let mut packed = Vec::with_capacity(chunk.cardinality());
            for row in chunk.rows() {
                packed.push(pack_pk_row(row)?);
            }
            Ok(stream::iter(packed.into_iter().map(Ok)))
        })
        .try_flatten();
    Ok(Box::pin(stream))
}

/// Pack a PK row (already projected to the PK columns in identifier order) into the same memcmp
/// byte layout as `_rw_pack_pk`: `[FORMAT_VERSION, memcmp_encode(col0), memcmp_encode(col1), ...]`,
/// each ascending. The `_rw_extra_pk` Bytea is included as one column and encoded once (no
/// double-encoding) — `_rw_pack_pk` already produced its bytes upstream.
fn pack_pk_row(row: impl Row) -> Result<Bytes> {
    let mut buf = Vec::new();
    buf.push(PK_FORMAT_VERSION);
    for datum in row.iter() {
        let encoded = memcmp_encoding::encode_value(datum, OrderType::ascending())
            .map_err(|e| anyhow!(e).context("memcmp encode PK column"))?;
        buf.extend_from_slice(encoded.as_ref());
    }
    Ok(Bytes::from(buf))
}

/// `dead = positions_n \ positions_r`.
fn diff_positions(positions_n: &HashSet<u64>, positions_r: &HashSet<u64>) -> HashSet<u64> {
    positions_n.difference(positions_r).copied().collect()
}

/// Classify one output row: a hit (dead, needs a DV) iff its packed PK is in the dead-PK set.
fn classify_hit(packed_pk: &Bytes, dead_pks: &HashSet<Bytes>) -> bool {
    dead_pks.contains(packed_pk)
}

/// Write a Puffin DV file for the given hit positions of an output data file, returning its
/// `DataFile`. Mirrors `dv_handler_impl::write_dv_puffin_file` (minus the existing-DV merge, since
/// output files are freshly written and carry no prior DV).
async fn write_output_dv_puffin_file(
    table: &Table,
    output_data_file: &DataFile,
    delete_vector: DeleteVector,
    location_generator: &DefaultLocationGenerator,
    file_name_generator: &mut DefaultFileNameGenerator,
) -> Result<DataFile> {
    let data_file_path = output_data_file.file_path().to_owned();
    let partition_key = build_partition_key(table, output_data_file)?;

    let file_name = file_name_generator.generate_file_name();
    let location = location_generator.generate_location(partition_key.as_ref(), &file_name);
    let output_file = table
        .file_io()
        .new_output(&location)
        .context("create output DV puffin file")?;
    let mut writer = PuffinWriter::new(&output_file, HashMap::new(), false)
        .await
        .context("create puffin writer")?;

    let cardinality = delete_vector.len();
    let properties = HashMap::from([
        (
            DELETION_VECTOR_PROPERTY_CARDINALITY.to_owned(),
            cardinality.to_string(),
        ),
        (
            DELETION_VECTOR_PROPERTY_REFERENCED_DATA_FILE.to_owned(),
            data_file_path.clone(),
        ),
    ]);
    let blob = delete_vector
        .to_puffin_blob(properties)
        .context("build DV puffin blob")?;
    writer
        .add(blob, CompressionCodec::None)
        .await
        .context("add DV puffin blob")?;
    let result = writer
        .close_with_metadata()
        .await
        .context("close DV puffin writer")?;
    let blob_metadata = result
        .blobs_metadata
        .first()
        .context("DV blob metadata should be present")?;

    let mut builder = DataFileBuilder::default();
    builder
        .content(DataContentType::PositionDeletes)
        .file_path(location)
        .file_format(DataFileFormat::Puffin)
        .record_count(cardinality)
        .file_size_in_bytes(result.file_size_in_bytes)
        .referenced_data_file(Some(data_file_path))
        .content_offset(Some(blob_metadata.offset() as i64))
        .content_size_in_bytes(Some(blob_metadata.length() as i64));
    if let Some(partition_key) = partition_key {
        builder
            .partition(partition_key.data().clone())
            .partition_spec_id(partition_key.spec().spec_id());
    }
    builder
        .build()
        .map_err(|e| anyhow!(e).context("build output DV DataFile"))
}

/// Build the partition key of a data file (None when unpartitioned). Mirrors
/// `dv_handler_impl::build_partition_key_from_data_file`, but returns `None` for unpartitioned
/// tables so the DV writer skips partition metadata.
fn build_partition_key(table: &Table, data_file: &DataFile) -> Result<Option<PartitionKey>> {
    let spec_id = data_file.partition_spec_id();
    let partition_spec = table
        .metadata()
        .partition_spec_by_id(spec_id)
        .with_context(|| format!("partition spec {} not found", spec_id))?;
    if partition_spec.is_unpartitioned() {
        return Ok(None);
    }
    Ok(Some(PartitionKey::new(
        partition_spec.as_ref().clone(),
        table.metadata().current_schema().clone(),
        data_file.partition().clone(),
    )))
}

#[cfg(test)]
mod tests {
    use iceberg::spec::{NestedField, PrimitiveType, Type};
    use risingwave_common::row::OwnedRow;
    use risingwave_common::types::ScalarImpl;

    use super::*;

    fn test_schema() -> Schema {
        // Field ids deliberately out of order vs. declaration to prove the resolver preserves the
        // caller's `pk_column_names` order, not the schema's field order.
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

    #[test]
    fn test_pack_pk_golden_and_deterministic() {
        // Known row -> stable, deterministic bytes that start with the format version.
        let row = OwnedRow::new(vec![
            Some(ScalarImpl::Int32(1)),
            Some(ScalarImpl::Utf8("a".into())),
        ]);
        let b1 = pack_pk_row(&row).unwrap();
        let b2 = pack_pk_row(&row).unwrap();
        assert_eq!(b1, b2, "packing the same row twice must be identical");
        assert_eq!(b1[0], PK_FORMAT_VERSION, "format version byte must lead");

        // Dynamic golden: version + ascending memcmp of each column, in order. Catches order/version
        // regressions against the encoder's own current output.
        let mut expected = vec![PK_FORMAT_VERSION];
        for datum in row.iter() {
            let enc = memcmp_encoding::encode_value(datum, OrderType::ascending()).unwrap();
            expected.extend_from_slice(enc.as_ref());
        }
        assert_eq!(b1.as_ref(), expected.as_slice());

        // Hardcoded golden: pin the exact on-wire bytes for this known row so the test is not merely
        // circular against its own encoder. Layout: `0x01` version lead, then Int32(1) ascending
        // memcmp (`00 80 00 00 01`: not-null tag + sign-flipped i32), then Utf8("a") ascending
        // memcmp. Any drift in version byte, column order, or per-column encoding flips this.
        let golden: Vec<u8> = vec![1, 0, 128, 0, 0, 1, 0, 1, 97, 0, 0, 0, 0, 0, 0, 0, 1];
        assert_eq!(b1.as_ref(), golden.as_slice());
    }

    #[test]
    fn test_pack_pk_distinguishes_rows() {
        let r1 = OwnedRow::new(vec![Some(ScalarImpl::Int32(1))]);
        let r2 = OwnedRow::new(vec![Some(ScalarImpl::Int32(2))]);
        assert_ne!(pack_pk_row(&r1).unwrap(), pack_pk_row(&r2).unwrap());
    }

    #[test]
    fn test_pack_pk_extra_pk_bytea_single_encode() {
        // `_rw_extra_pk` arrives as an already-packed Bytea; the resolver encodes it once, like any
        // other PK column. Two distinct packed values must stay distinct.
        let a = OwnedRow::new(vec![Some(ScalarImpl::Bytea(Box::new([0x01, 0x02])))]);
        let b = OwnedRow::new(vec![Some(ScalarImpl::Bytea(Box::new([0x01, 0x03])))]);
        assert_ne!(pack_pk_row(&a).unwrap(), pack_pk_row(&b).unwrap());
    }

    #[test]
    fn test_diff_positions_is_set_difference() {
        let dv_r: HashSet<u64> = [1, 3, 5].into_iter().collect();
        let dv_n: HashSet<u64> = [1, 2, 3, 4, 5, 6].into_iter().collect();
        let dead = diff_positions(&dv_n, &dv_r);
        let expected: HashSet<u64> = [2, 4, 6].into_iter().collect();
        assert_eq!(dead, expected);
    }

    #[test]
    fn test_diff_positions_empty_when_no_new_deletes() {
        let dv_r: HashSet<u64> = [1, 2, 3].into_iter().collect();
        let dv_n: HashSet<u64> = [1, 2, 3].into_iter().collect();
        assert!(diff_positions(&dv_n, &dv_r).is_empty());
    }

    #[test]
    fn test_classify_partitions_hits_and_misses() {
        let mk = |b: &[u8]| Bytes::copy_from_slice(b);
        let dead: HashSet<Bytes> = [mk(b"a"), mk(b"c")].into_iter().collect();

        let rows = vec![
            (mk(b"a"), "f", 0u64),
            (mk(b"b"), "f", 1u64),
            (mk(b"c"), "f", 2u64),
            (mk(b"d"), "f", 3u64),
        ];

        let mut hits = Vec::new();
        let mut misses = Vec::new();
        for (pk, file, pos) in rows {
            if classify_hit(&pk, &dead) {
                hits.push(pos);
            } else {
                misses.push((pk, file, pos));
            }
        }
        assert_eq!(hits, vec![0, 2]);
        assert_eq!(
            misses.iter().map(|(_, _, p)| *p).collect::<Vec<_>>(),
            vec![1, 3]
        );
    }
}
