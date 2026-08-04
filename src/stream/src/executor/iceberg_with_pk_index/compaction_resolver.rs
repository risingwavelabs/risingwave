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

use std::iter::Peekable;

use anyhow::{Context, anyhow};
use hashbrown::{HashMap, HashSet};
use iceberg::delete_vector::DeleteVector;
use iceberg::io::FileIO;
use iceberg::spec::{
    DataContentType, DataFileFormat, FormatVersion, ManifestContentType, SnapshotRef,
};
use iceberg::table::Table;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use parquet::arrow::{ParquetRecordBatchStreamBuilder, ProjectionMask};
use parquet::file::properties::WriterProperties;
use risingwave_common::array::DataChunk;
use risingwave_common::array::arrow::IcebergArrowConvert;
use risingwave_common::id::SinkId;
use risingwave_common::row::RowExt;
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
use risingwave_connector::sink::SinkError;
use risingwave_connector::sink::iceberg::{
    IcebergConfig, IcebergPositionDeleteCommitResult, PARQUET_CREATED_BY,
    read_position_deletes_from_file, serialize_data_files_default_spec, write_dv_puffin_file,
    write_parquet_position_delete_file,
};
use risingwave_connector::source::iceberg::ParquetFileReader;
use risingwave_pb::connector_service::SinkMetadata;
use risingwave_pb::stream_service::PbIcebergPkIndexSinkRole;
use tokio::sync::mpsc::UnboundedReceiver;
use uuid::Uuid;

use crate::executor::prelude::*;
use crate::task::LocalBarrierManager;

/// Leaf executor for the iceberg pk-index coordinated compaction (see [`resolve_compaction`]).
///
/// Like the streaming pipeline's other pk-index executors it has **no stream input**. One resolver
/// runs in each vnode-aligned actor of a transient independent job and is driven by barriers
/// delivered directly through `barrier_receiver`. It does NOT own the pk-index state table — it
/// only produces resolved SURVIVOR output chunks that a downstream `Writer_B` consumes to rebuild
/// the index. The actor owning vnode 0 AUTHORS all conflict deletion-vector files and reports them
/// straight to meta.
///
/// Lifecycle:
/// - On the first barrier: forward it (no state to init).
/// - After resolve completes, on the next barrier (the stop barrier): REPORT the authored conflict DV
///   files to meta, forward the barrier, and exit. The meta job collects the report via
///   `collect_delete_reports` from the partial-graph barrier response. A resolve error aborts the
///   executor (the job then fails and the compaction is abandoned/retried; the whole sequence is
///   idempotent).
///
/// # Output chunk contract
///
/// Schema: `[pk_columns.., file_path: Varchar, position: Int64]`. Every emitted row is
/// a SURVIVOR: `Writer_B` upserts `pk -> (file_path, position)` in the index (index-only; the data
/// already lives in the compaction output file). Conflicts (rows killed during the window) are NOT
/// emitted here — the resolver authors their masking deletion vectors and reports them to meta.
pub struct CompactionResolverExecutor {
    ctx: ActorContextRef,
    sink_id: SinkId,
    iceberg_config: IcebergConfig,
    pk_indices: Vec<usize>,
    pk_data_types: Vec<DataType>,
    output_data_file_paths: Vec<String>,
    input_data_file_paths: Vec<String>,
    read_snapshot_id: i64,
    chunk_size: usize,
    local_barrier_manager: LocalBarrierManager,
    barrier_receiver: UnboundedReceiver<Barrier>,
}

impl CompactionResolverExecutor {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        ctx: ActorContextRef,
        sink_id: SinkId,
        iceberg_config: IcebergConfig,
        pk_indices: Vec<usize>,
        pk_data_types: Vec<DataType>,
        output_data_file_paths: Vec<String>,
        input_data_file_paths: Vec<String>,
        read_snapshot_id: i64,
        chunk_size: usize,
        local_barrier_manager: LocalBarrierManager,
        barrier_receiver: UnboundedReceiver<Barrier>,
    ) -> Self {
        Self {
            ctx,
            sink_id,
            iceberg_config,
            pk_indices,
            pk_data_types,
            output_data_file_paths,
            input_data_file_paths,
            read_snapshot_id,
            chunk_size,
            local_barrier_manager,
            barrier_receiver,
        }
    }

    /// Data types of the survivor output chunk: `[pk_columns.., file_path, position]`.
    fn output_data_types(&self) -> Vec<DataType> {
        let mut types = self.pk_data_types.clone();
        types.push(DataType::Varchar);
        types.push(DataType::Int64);
        types
    }

    #[try_stream(ok = DataChunk, error = SinkError)]
    async fn resolve<'a>(&'a self, conflict_delete_metadata: &'a mut Option<SinkMetadata>) {
        let table = self.iceberg_config.load_table().await?;
        let file_io = table.file_io();
        let snapshot_r = table
            .metadata()
            .snapshot_by_id(self.read_snapshot_id)
            .with_context(|| {
                format!(
                    "compaction read snapshot {} not present in table metadata",
                    self.read_snapshot_id
                )
            })?;
        let snapshot_n = table
            .metadata()
            .current_snapshot()
            .context("table has no current snapshot to resolve compaction against")?;

        let input_paths: HashSet<&str> = self
            .input_data_file_paths
            .iter()
            .map(String::as_str)
            .collect();

        // Deletion-vector positions per input data file, at R (baseline) and at N (post-window).
        let dv_r = collect_input_dvs(&table, snapshot_r, &input_paths).await?;
        let dv_n = collect_input_dvs(&table, snapshot_n, &input_paths).await?;

        // Selectively re-read the input data files at the delete-diff positions to recover the primary
        // keys deleted during the window. `pk` datums are keyed by an `OwnedRow` so membership tests
        // against the output scan below are exact.
        let mut delete_diff_pk_set: HashSet<OwnedRow> = HashSet::new();
        let stream =
            futures::stream::iter(self.input_data_file_paths.iter().map(|input_path| async {
                let Some(n_positions) = dv_n.get(input_path) else {
                    return Ok(Vec::new());
                };
                let r_positions = dv_r.get(input_path);
                let mut diff = n_positions.clone();
                if let Some(r_positions) = r_positions {
                    diff -= r_positions;
                }
                if diff.is_empty() {
                    return Ok(Vec::new());
                }
                scan_input_pks_at_positions(file_io, input_path, &self.pk_indices, &diff).await
            }))
            .buffer_unordered(8);
        #[for_await]
        for pks in stream {
            delete_diff_pk_set.extend(pks?);
        }

        // Scan every output data file once per actor.
        let mut conflicts: HashMap<String, DeleteVector> = HashMap::new();
        #[for_await]
        for chunk in self.scan_output_file(file_io, &delete_diff_pk_set, &mut conflicts) {
            yield chunk?;
        }

        *conflict_delete_metadata =
            write_conflict_delete_files(&self.iceberg_config, &table, conflicts).await?;
    }

    #[try_stream(ok = DataChunk, error = SinkError)]
    async fn scan_output_file<'a>(
        &'a self,
        file_io: &'a FileIO,
        delete_diff_pk_set: &'a HashSet<OwnedRow>,
        conflicts: &'a mut HashMap<String, DeleteVector>,
    ) {
        let mut buffer = DataChunkBuilder::new(self.output_data_types(), self.chunk_size);
        for file in &self.output_data_file_paths {
            #[for_await]
            for chunk in scan_output_file_inner(
                file_io,
                file,
                &self.pk_indices,
                delete_diff_pk_set,
                &mut buffer,
                conflicts,
            ) {
                yield chunk?;
            }
        }
        if let Some(chunk) = buffer.consume_all() {
            yield chunk;
        }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(mut self) {
        let actor_id = self.ctx.id;

        // First barrier: forward it. There is no state table to initialize.
        let first_barrier = self.barrier_receiver.recv().await.ok_or_else(|| {
            StreamExecutorError::channel_closed("compaction resolver barrier receiver")
        })?;
        yield Message::Barrier(first_barrier);

        let mut conflict_delete_metadata = None;
        #[for_await]
        for chunk in self.resolve(&mut conflict_delete_metadata) {
            let chunk = chunk.map_err(|e| (e, self.sink_id))?;
            yield Message::Chunk(chunk.into());
        }

        let Some(barrier) = self.barrier_receiver.recv().await else {
            return Err(StreamExecutorError::channel_closed(
                "compaction resolver barrier receiver",
            ));
        };
        if let Some(metadata) = conflict_delete_metadata
            && metadata.metadata.is_some()
        {
            self.local_barrier_manager
                .report_iceberg_pk_index_sink_metadata(
                    barrier.epoch,
                    self.sink_id,
                    actor_id,
                    PbIcebergPkIndexSinkRole::CompactionResolver,
                    Some(metadata),
                );
        }
        yield Message::Barrier(barrier);
    }
}

impl Execute for CompactionResolverExecutor {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.execute_inner().boxed()
    }
}

/// Walks the manifests of `snapshot` and collects, per input data file, the merged set of deleted
/// positions from every live position-delete file (V3 Puffin DV or V2 Parquet) referencing it.
async fn collect_input_dvs(
    table: &Table,
    snapshot: &SnapshotRef,
    input_paths: &HashSet<&str>,
) -> Result<HashMap<String, DeleteVector>, SinkError> {
    let file_io = table.file_io();
    let mut map: HashMap<String, DeleteVector> = HashMap::new();

    let manifest_list = snapshot
        .load_manifest_list(file_io, table.metadata())
        .await
        .map_err(|e| anyhow!(e).context("load manifest list for compaction resolve"))?;

    for manifest_file in manifest_list.entries() {
        // Only delete manifests can carry position-delete files.
        if manifest_file.content != ManifestContentType::Deletes {
            continue;
        }
        let manifest = manifest_file
            .load_manifest(file_io)
            .await
            .map_err(|e| anyhow!(e).context("load delete manifest for compaction resolve"))?;
        for entry in manifest.entries() {
            if !entry.is_alive() {
                continue;
            }
            let data_file = entry.data_file();
            if data_file.content_type() == DataContentType::PositionDeletes
                && let Some(referenced) = data_file.referenced_data_file()
                && input_paths.contains(referenced.as_str())
            {
                let positions = read_position_deletes_from_file(file_io, data_file)
                    .await
                    .context("read input deletion vector for compaction resolve")?;
                if map.insert(referenced, positions).is_some() {
                    return Err(anyhow!(
                        "input data file {} has multiple live position-delete files in snapshot {}",
                        data_file.referenced_data_file().unwrap(),
                        snapshot.snapshot_id()
                    )
                    .into());
                }
            }
        }
    }
    Ok(map)
}

/// Full-scans `path`, and for every row whose 0-based position is in `want_positions`, inserts its
/// pk into `out`.
async fn scan_input_pks_at_positions(
    file_io: &FileIO,
    path: &str,
    pk_indices: &[usize],
    want_positions: &DeleteVector,
) -> Result<Vec<OwnedRow>, SinkError> {
    let mut results = Vec::new();
    let input_file = file_io.new_input(path)?;
    let metadata = input_file.metadata().await?;
    let reader = input_file.reader().await?;
    let builder = ParquetRecordBatchStreamBuilder::new(ParquetFileReader::new(metadata, reader))
        .await
        .map_err(|e| anyhow!(e).context(format!("open parquet reader for {path}")))?;
    let projection = ProjectionMask::roots(builder.parquet_schema(), pk_indices.iter().copied());
    let mut stream = builder
        .with_projection(projection)
        .build()
        .map_err(|e| anyhow!(e).context(format!("build parquet stream for {path}")))?;

    let mut position = 0;
    let mut iter = want_positions.iter().peekable();
    while let Some(batch) = stream.next().await {
        let batch =
            batch.map_err(|e| anyhow!(e).context(format!("read parquet batch of {path}")))?;
        let chunk = IcebergArrowConvert
            .chunk_from_record_batch(&batch)
            .map_err(|e| anyhow!(e).context(format!("convert parquet batch of {path}")))?;
        let need =
            take_positions_before(&mut iter, (position + chunk.capacity()).try_into().unwrap())
                .map(|pos| pos as usize - position);
        for pos in need {
            let pk = chunk.row_at(pos).0.to_owned_row();
            results.push(pk);
        }
        position += chunk.capacity();
    }

    Ok(results)
}

fn take_positions_before<'a, I>(
    positions: &'a mut Peekable<I>,
    end_exclusive: u64,
) -> impl Iterator<Item = u64> + 'a
where
    I: Iterator<Item = u64> + 'a,
{
    std::iter::from_fn(move || positions.next_if(|&position| position < end_exclusive))
}

#[try_stream(ok = DataChunk, error = SinkError)]
async fn scan_output_file_inner<'a>(
    file_io: &'a FileIO,
    path: &'a str,
    pk_indices: &'a [usize],
    delete_diff_pk_set: &'a HashSet<OwnedRow>,
    buffer: &'a mut DataChunkBuilder,
    conflicts: &'a mut HashMap<String, DeleteVector>,
) {
    let input_file = file_io.new_input(path)?;
    let metadata = input_file.metadata().await?;
    let reader = input_file.reader().await?;
    let builder = ParquetRecordBatchStreamBuilder::new(ParquetFileReader::new(metadata, reader))
        .await
        .map_err(|e| anyhow!(e).context(format!("open parquet reader for {path}")))?;
    let projection = ProjectionMask::roots(builder.parquet_schema(), pk_indices.iter().copied());
    let stream = builder
        .with_projection(projection)
        .build()
        .map_err(|e| anyhow!(e).context(format!("build parquet stream for {path}")))?;

    let mut position: i64 = 0;

    #[for_await]
    for batch in stream {
        let batch =
            batch.map_err(|e| anyhow!(e).context(format!("read parquet batch of {path}")))?;
        let chunk = IcebergArrowConvert
            .chunk_from_record_batch(&batch)
            .map_err(|e| anyhow!(e).context(format!("convert parquet batch of {path}")))?;
        for row_idx in 0..chunk.capacity() {
            let pk = chunk.row_at(row_idx).0.into_owned_row();
            let row_position = position + row_idx as i64;
            if delete_diff_pk_set.contains(&pk) {
                conflicts
                    .entry_ref(path)
                    .or_default()
                    .insert(row_position as u64);
            } else if let Some(full_chunk) = buffer.append_one_row(pk.chain([
                Some(ScalarRefImpl::Utf8(path)),
                Some(ScalarRefImpl::Int64(row_position)),
            ])) {
                yield full_chunk;
            }
        }
        position += chunk.capacity() as i64;
    }
}

async fn write_conflict_delete_files(
    config: &IcebergConfig,
    table: &Table,
    conflicts: HashMap<String, DeleteVector>,
) -> Result<Option<SinkMetadata>, SinkError> {
    if conflicts.is_empty() {
        return Ok(None);
    }

    let location_generator = DefaultLocationGenerator::new(table.metadata().clone())?;
    // A fresh uuid per resolve keeps file names unique across the vnode-aligned resolver actors.
    let uuid_suffix = Uuid::now_v7();
    let puffin_file_name_generator = DefaultFileNameGenerator::new(
        "resolver".to_owned(),
        Some(format!("delvec-{uuid_suffix}")),
        DataFileFormat::Puffin,
    );
    let parquet_file_name_generator = DefaultFileNameGenerator::new(
        "resolver".to_owned(),
        Some(format!("pos-del-{uuid_suffix}")),
        DataFileFormat::Parquet,
    );
    let use_puffin = table.metadata().format_version() >= FormatVersion::V3;

    let mut delete_files = Vec::with_capacity(conflicts.len());
    for (output_file, delete_vector) in conflicts {
        let file = if use_puffin {
            write_dv_puffin_file(
                table,
                &location_generator,
                &puffin_file_name_generator,
                output_file,
                delete_vector,
                None,
            )
            .await?
        } else {
            let parquet_writer_properties = WriterProperties::builder()
                .set_compression(config.get_parquet_compression())
                .set_max_row_group_bytes(config.write_parquet_max_row_group_bytes())
                .set_created_by(PARQUET_CREATED_BY.to_owned())
                .build();
            write_parquet_position_delete_file(
                table,
                &location_generator,
                &parquet_file_name_generator,
                parquet_writer_properties,
                output_file,
                delete_vector,
                None,
            )
            .await?
        };
        delete_files.push(file);
    }

    let delete_files = serialize_data_files_default_spec(table, delete_files)?;
    let result = IcebergPositionDeleteCommitResult {
        schema_id: table.metadata().current_schema_id(),
        partition_spec_id: table.metadata().default_partition_spec_id(),
        delete_files,
        // The output files are uncommitted, so no pre-existing DV file is being overwritten.
        overwrite_files: vec![],
    };
    let sink_metadata = SinkMetadata::try_from(&result)?;
    Ok(Some(sink_metadata))
}

#[cfg(test)]
mod tests {
    use super::take_positions_before;

    #[test]
    fn test_take_positions_before_preserves_next_batch_position() {
        let mut positions = vec![1, 3].into_iter().peekable();

        assert_eq!(
            take_positions_before(&mut positions, 2).collect::<Vec<_>>(),
            vec![1]
        );
        assert_eq!(
            take_positions_before(&mut positions, 4).collect::<Vec<_>>(),
            vec![3]
        );
    }
}
