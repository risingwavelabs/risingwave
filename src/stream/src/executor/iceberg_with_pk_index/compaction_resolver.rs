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
use parquet::schema::types::SchemaDescriptor;
use risingwave_common::array::DataChunk;
use risingwave_common::array::arrow::IcebergArrowConvert;
use risingwave_common::id::SinkId;
use risingwave_common::row::RowExt;
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
use risingwave_connector::sink::SinkError;
use risingwave_connector::sink::iceberg::{
    IcebergConfig, IcebergPositionDeleteCommitResult, read_position_deletes_from_file,
    serialize_data_files_default_spec, write_dv_puffin_file, write_parquet_position_delete_file,
};
use risingwave_connector::source::iceberg::ParquetFileReader;
use risingwave_pb::connector_service::SinkMetadata;
use risingwave_pb::id::IcebergCompactionTaskId;
use risingwave_pb::stream_plan::iceberg_pk_index_compaction_context::{Phase, ResolverTaskInput};
use risingwave_pb::stream_service::PbIcebergPkIndexSinkRole;
use risingwave_rpc_client::MetaClient;
use tokio::sync::mpsc::UnboundedReceiver;
use uuid::Uuid;

use super::load_table_at_least;
use crate::executor::prelude::*;
use crate::task::LocalBarrierManager;

/// Leaf executor for the iceberg pk-index coordinated compaction.
///
/// It has **no stream input**. The actor is part of the sink's static graph but only runs while a
/// compaction is being applied. It is driven by barriers delivered directly through
/// `barrier_receiver` and produces resolved survivor rows for the downstream writer.
///
/// Lifecycle:
/// - On the first barrier: forward it (no state to init).
/// - After resolve completes, on the next barrier (the end barrier): REPORT the conflict DV
///   files to meta, forward the barrier, and exit.
///
/// # Output chunk contract
///
/// Schema: `[pk_columns.., file_path: Varchar, position: Int64]`. Every emitted row is
/// a SURVIVOR: `Writer` upserts `pk -> (file_path, position)` in the index (index-only; the data
/// already lives in the compaction output file). Conflicts (rows killed during the window) are NOT
/// emitted here.
pub struct CompactionResolverExecutor {
    ctx: ActorContextRef,
    sink_id: SinkId,
    iceberg_config: IcebergConfig,
    pk_indices: Vec<usize>,
    pk_data_types: Vec<DataType>,
    chunk_size: usize,
    local_barrier_manager: LocalBarrierManager,
    barrier_receiver: UnboundedReceiver<Barrier>,
    meta_client: MetaClient,
}

fn resolver_task_from_initial_barrier(
    sink_id: SinkId,
    barrier: &Barrier,
) -> StreamExecutorResult<(IcebergCompactionTaskId, ResolverTaskInput)> {
    match barrier.iceberg_pk_index_compaction() {
        Some(context)
            if barrier.is_checkpoint()
                && context.sink_id == sink_id
                && context.phase == Phase::Begin as i32 =>
        {
            let task_input = context.resolver_task_input.clone().ok_or_else(|| {
                StreamExecutorError::from(anyhow!(
                    "compaction resolver sink {} task {} missing resolver task input",
                    sink_id,
                    context.task_id
                ))
            })?;
            Ok((context.task_id, task_input))
        }
        _ => Err(StreamExecutorError::from(anyhow!(
            "compaction resolver sink {} expected initial begin barrier, got {:?}",
            sink_id,
            barrier
        ))),
    }
}

fn validate_resolver_end_barrier(
    sink_id: SinkId,
    barrier: &Barrier,
    begin: &Barrier,
    task_id: IcebergCompactionTaskId,
) -> StreamExecutorResult<()> {
    match barrier.iceberg_pk_index_compaction() {
        Some(context)
            if barrier.is_checkpoint()
                && barrier.epoch.prev == begin.epoch.curr
                && context.sink_id == sink_id
                && context.task_id == task_id
                && context.phase == Phase::End as i32
                && context.resolver_task_input.is_none() =>
        {
            Ok(())
        }
        _ => Err(StreamExecutorError::from(anyhow!(
            "compaction resolver sink {} task {} expected end barrier, got {:?}",
            sink_id,
            task_id,
            barrier
        ))),
    }
}

impl CompactionResolverExecutor {
    #[expect(clippy::too_many_arguments)]
    pub fn new(
        ctx: ActorContextRef,
        sink_id: SinkId,
        iceberg_config: IcebergConfig,
        pk_indices: Vec<usize>,
        pk_data_types: Vec<DataType>,
        chunk_size: usize,
        local_barrier_manager: LocalBarrierManager,
        barrier_receiver: UnboundedReceiver<Barrier>,
        meta_client: MetaClient,
    ) -> Self {
        Self {
            ctx,
            sink_id,
            iceberg_config,
            pk_indices,
            pk_data_types,
            chunk_size,
            local_barrier_manager,
            barrier_receiver,
            meta_client,
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
    async fn resolve<'a>(
        &'a self,
        task_input: &'a ResolverTaskInput,
        expected_snapshot: Option<i64>,
        conflict_delete_metadata: &'a mut Option<SinkMetadata>,
    ) {
        let table = load_table_at_least(&self.iceberg_config, expected_snapshot).await?;
        let file_io = table.file_io();
        let snapshot_r = table
            .metadata()
            .snapshot_by_id(task_input.read_snapshot_id)
            .with_context(|| {
                format!(
                    "compaction read snapshot {} not present in table metadata",
                    task_input.read_snapshot_id
                )
            })?;
        let snapshot_n = table
            .metadata()
            .current_snapshot()
            .context("table has no current snapshot to resolve compaction against")?;

        let input_paths: HashSet<&str> = task_input
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
        let stream = futures::stream::iter(task_input.input_data_file_paths.iter().map(
            |input_path| async {
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
            },
        ))
        .buffer_unordered(8);
        #[for_await]
        for pks in stream {
            delete_diff_pk_set.extend(pks?);
        }

        // Scan every output data file once per actor.
        let mut conflicts: HashMap<String, DeleteVector> = HashMap::new();
        #[for_await]
        for chunk in self.scan_output_file(
            file_io,
            &task_input.output_data_file_paths,
            &delete_diff_pk_set,
            &mut conflicts,
        ) {
            yield chunk?;
        }

        *conflict_delete_metadata =
            write_conflict_delete_files(&self.iceberg_config, &table, conflicts).await?;
    }

    #[try_stream(ok = DataChunk, error = SinkError)]
    async fn scan_output_file<'a>(
        &'a self,
        file_io: &'a FileIO,
        output_data_file_paths: &'a [String],
        delete_diff_pk_set: &'a HashSet<OwnedRow>,
        conflicts: &'a mut HashMap<String, DeleteVector>,
    ) {
        let mut buffer = DataChunkBuilder::new(self.output_data_types(), self.chunk_size);
        for file in output_data_file_paths {
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
        let (task_id, task_input) =
            resolver_task_from_initial_barrier(self.sink_id, &first_barrier)?;
        let begin_barrier = first_barrier.clone();
        yield Message::Barrier(first_barrier);

        let expected_snapshot = self
            .meta_client
            .wait_iceberg_pk_index_sink_epoch(self.sink_id, begin_barrier.epoch.prev)
            .await?;

        let mut conflict_delete_metadata = None;
        #[for_await]
        for chunk in self.resolve(
            &task_input,
            expected_snapshot,
            &mut conflict_delete_metadata,
        ) {
            let chunk = chunk.map_err(|e| (e, self.sink_id))?;
            yield Message::Chunk(chunk.into());
        }

        let Some(barrier) = self.barrier_receiver.recv().await else {
            return Err(StreamExecutorError::channel_closed(
                "compaction resolver barrier receiver",
            ));
        };
        validate_resolver_end_barrier(self.sink_id, &barrier, &begin_barrier, task_id)?;
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
/// positions from every live position-delete file referencing it.
async fn collect_input_dvs(
    table: &Table,
    snapshot: &SnapshotRef,
    input_paths: &HashSet<&str>,
) -> Result<HashMap<String, DeleteVector>, SinkError> {
    let file_io = table.file_io();
    let mut map: HashMap<String, DeleteVector> = HashMap::new();

    let manifest_list = table
        .object_cache()
        .get_manifest_list(snapshot, &table.metadata_ref())
        .await?;

    for manifest_file in manifest_list.entries() {
        // Only delete manifests can carry position-delete files.
        if manifest_file.content != ManifestContentType::Deletes {
            continue;
        }
        let manifest = manifest_file.load_manifest(file_io).await?;
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
                    .map_err(SinkError::Iceberg)?;
                if map.insert(referenced, positions).is_some() {
                    return Err(SinkError::Iceberg(anyhow!(
                        "input data file {} has multiple live position-delete files in snapshot {}",
                        data_file.referenced_data_file().unwrap(),
                        snapshot.snapshot_id()
                    )));
                }
            }
        }
    }
    Ok(map)
}

/// Builds a projection for PK root columns and maps the projected physical order back to the
/// downstream PK order.
fn pk_projection(
    parquet_schema: &SchemaDescriptor,
    pk_indices: &[usize],
) -> Result<(ProjectionMask, Vec<usize>), SinkError> {
    let root_count = parquet_schema.root_schema().get_fields().len();
    let mut physical_indices = Vec::with_capacity(pk_indices.len());
    let mut seen = HashSet::with_capacity(pk_indices.len());

    for &pk_index in pk_indices {
        if pk_index >= root_count {
            return Err(SinkError::Iceberg(anyhow!(
                "pk index {pk_index} is out of range for parquet schema with {root_count} root columns"
            )));
        }
        if !seen.insert(pk_index) {
            return Err(SinkError::Iceberg(anyhow!("duplicate pk index {pk_index}")));
        }
        physical_indices.push(pk_index);
    }

    physical_indices.sort_unstable();
    let pk_order = pk_indices
        .iter()
        .map(|pk_index| {
            physical_indices
                .binary_search(pk_index)
                .expect("validated pk index must be projected")
        })
        .collect();
    let projection = ProjectionMask::roots(parquet_schema, physical_indices);

    Ok((projection, pk_order))
}

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
    let (projection, pk_order) = pk_projection(builder.parquet_schema(), pk_indices)?;
    let mut stream = builder
        .with_projection(projection)
        .build()
        .map_err(|e| anyhow!(e).context(format!("build parquet stream for {path}")))?;

    let mut base_pos = 0;
    let mut iter = want_positions.iter().peekable();
    while let Some(batch) = stream.next().await {
        let batch =
            batch.map_err(|e| anyhow!(e).context(format!("read parquet batch of {path}")))?;
        let chunk = IcebergArrowConvert
            .chunk_from_record_batch(&batch)
            .map_err(|e| anyhow!(e).context(format!("convert parquet batch of {path}")))?
            .project(&pk_order);
        while let Some(&iter_pos) = iter.peek() {
            let chunk_pos = iter_pos as usize - base_pos;
            if chunk_pos >= chunk.capacity() {
                break;
            }
            let pk = chunk.row_at(chunk_pos).0.to_owned_row();
            results.push(pk);
            iter.next();
        }
        base_pos += chunk.capacity();
    }

    Ok(results)
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
    let (projection, pk_order) = pk_projection(builder.parquet_schema(), pk_indices)?;
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
            .map_err(|e| anyhow!(e).context(format!("convert parquet batch of {path}")))?
            .project(&pk_order);
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

    let location_generator = DefaultLocationGenerator::new(table.metadata())?;
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
        // Compaction outputs are not committed yet and the resolver only receives their paths.
        // Meta retains the full output data-file metadata through B2 and backfills each
        // resolver/merger delete file's partition from its referenced data file during pre-commit.
        // Therefore these provisional delete artifacts intentionally omit `PartitionKey` here.
        let file = if use_puffin {
            write_dv_puffin_file(
                table,
                &location_generator,
                &puffin_file_name_generator,
                output_file,
                &delete_vector,
                None,
            )
            .await?
        } else {
            write_parquet_position_delete_file(
                table,
                &location_generator,
                &parquet_file_name_generator,
                config,
                output_file,
                &delete_vector,
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
    use std::sync::Arc;

    use parquet::basic::Type as PhysicalType;
    use parquet::schema::types::{SchemaDescriptor, Type};
    use risingwave_common::array::DataChunkTestExt;
    use risingwave_common::util::epoch::test_epoch;
    use risingwave_pb::stream_plan::IcebergPkIndexCompactionContext;

    use super::*;

    fn test_parquet_schema() -> SchemaDescriptor {
        SchemaDescriptor::new(Arc::new(
            Type::group_type_builder("schema")
                .with_fields(
                    ["column_0", "column_1", "column_2", "column_3", "column_4"]
                        .into_iter()
                        .map(|name| {
                            Arc::new(
                                Type::primitive_type_builder(name, PhysicalType::INT32)
                                    .build()
                                    .unwrap(),
                            )
                        })
                        .collect(),
                )
                .build()
                .unwrap(),
        ))
    }

    #[test]
    fn test_pk_projection_restores_downstream_pk_order() {
        let parquet_schema = test_parquet_schema();

        let (_, order) = pk_projection(&parquet_schema, &[2, 0, 1]).unwrap();
        assert_eq!(order, vec![2, 0, 1]);

        let (_, order) = pk_projection(&parquet_schema, &[4, 1, 3]).unwrap();
        assert_eq!(order, vec![2, 0, 1]);
    }

    #[test]
    fn test_pk_projection_reorders_heterogeneous_chunk() {
        let chunk = DataChunk::from_pretty(
            "i T
             7 k",
        );
        let projected = chunk.project(&[1, 0]);

        assert_eq!(
            projected,
            DataChunk::from_pretty(
                "T i
                 k 7",
            )
        );
    }

    fn compaction_barrier(
        epoch: u64,
        phase: Phase,
        resolver_task_input: Option<ResolverTaskInput>,
    ) -> Barrier {
        Barrier::new_test_barrier(test_epoch(epoch)).with_iceberg_pk_index_compaction(
            IcebergPkIndexCompactionContext {
                sink_id: SinkId::new(7),
                task_id: 11.into(),
                phase: phase as i32,
                resolver_task_input,
            },
        )
    }

    #[test]
    fn test_resolver_task_is_initialized_from_b1() {
        let task_input = ResolverTaskInput {
            output_data_file_paths: vec!["output.parquet".into()],
            input_data_file_paths: vec!["input.parquet".into()],
            read_snapshot_id: 42,
        };
        let barrier = compaction_barrier(2, Phase::Begin, Some(task_input.clone()));

        let (task_id, actual) =
            resolver_task_from_initial_barrier(SinkId::new(7), &barrier).unwrap();
        assert_eq!(task_id, IcebergCompactionTaskId::from(11));
        assert_eq!(actual, task_input);
    }

    #[test]
    fn test_resolver_requires_task_input_only_on_b1() {
        let begin = compaction_barrier(2, Phase::Begin, Some(ResolverTaskInput::default()));
        let end = compaction_barrier(3, Phase::End, None);
        validate_resolver_end_barrier(SinkId::new(7), &end, &begin, 11.into()).unwrap();

        let invalid_begin = compaction_barrier(2, Phase::Begin, None);
        assert!(resolver_task_from_initial_barrier(SinkId::new(7), &invalid_begin).is_err());
        let invalid_end = compaction_barrier(3, Phase::End, Some(ResolverTaskInput::default()));
        assert!(
            validate_resolver_end_barrier(SinkId::new(7), &invalid_end, &begin, 11.into()).is_err()
        );
    }
}
