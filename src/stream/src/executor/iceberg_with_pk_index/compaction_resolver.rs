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

use std::time::Instant;

use anyhow::{Context, anyhow};
use futures::TryStreamExt;
use hashbrown::{HashMap, HashSet};
use iceberg::delete_vector::DeleteVector;
use iceberg::io::{FileIO, FileMetadata};
use iceberg::spec::{DataContentType, DataFile, ManifestContentType, SnapshotRef};
use iceberg::table::Table;
use iceberg::writer::file_writer::location_generator::DefaultLocationGenerator;
use parquet::arrow::arrow_reader::ArrowReaderMetadata;
use parquet::arrow::{ParquetRecordBatchStreamBuilder, ProjectionMask};
use parquet::schema::types::SchemaDescriptor;
use risingwave_common::array::arrow::IcebergArrowConvert;
use risingwave_common::array::{ArrayImpl, DataChunk, I64Array, Utf8Array};
use risingwave_common::bitmap::Bitmap;
use risingwave_common::id::SinkId;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_connector::sink::SinkError;
use risingwave_connector::sink::iceberg::{
    IcebergConfig, IcebergPositionDeleteCommitResult, PkIndexDataFileLocator,
    PositionDeleteFileNameGenerators, read_position_deletes_from_file,
    serialize_data_files_default_spec, write_position_delete_file,
};
use risingwave_connector::source::iceberg::ParquetFileReader;
use risingwave_expr::expr::{
    BoxedExpression, InputRefExpression, SyncExpressionBoxExt, build_func,
};
use risingwave_pb::connector_service::SinkMetadata;
use risingwave_pb::expr::expr_node::PbType;
use risingwave_pb::id::IcebergCompactionTaskId;
use risingwave_pb::stream_plan::iceberg_pk_index_compaction_context::{Phase, ResolverTaskInput};
use risingwave_pb::stream_service::PbIcebergPkIndexSinkRole;
use risingwave_rpc_client::MetaClient;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::{Semaphore, mpsc};
use tokio::task::JoinHandle;
use uuid::Uuid;

use super::load_table_at_least;
use crate::executor::prelude::*;
use crate::task::LocalBarrierManager;

/// Concurrent manifest loads when collecting the delete files of a snapshot.
const MANIFEST_LOAD_CONCURRENCY: usize = 16;
/// Concurrent input files whose delete diff is read and re-scanned.
const DELETE_DIFF_CONCURRENCY: usize = 16;
/// Concurrent output data files whose parquet footers are loaded to plan the scan.
const OUTPUT_FILE_OPEN_CONCURRENCY: usize = 16;
/// Concurrent row-group scans of the output data files while rebuilding the pk index.
const OUTPUT_SCAN_CONCURRENCY: usize = 8;
const OUTPUT_SCAN_CHANNEL_CAPACITY: usize = 16;

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
/// Schema: `[pk_columns.., data_file, position: Int64]`, where `data_file` is the output file
/// path (Varchar) in the legacy layout, or its uuid (Bytea) otherwise, matching the writer's PK
/// index. Every emitted row is a SURVIVOR: `Writer` upserts `pk -> (data_file, position)` in the
/// index (index-only; the data already lives in the compaction output file). Conflicts (rows
/// killed during the window) are NOT emitted here.
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

    /// Data types of the survivor output chunk: `[pk_columns.., data_file, position]`.
    fn output_data_types(&self) -> Vec<DataType> {
        let mut types = self.pk_data_types.clone();
        types.push(if self.iceberg_config.pk_index_stores_file_uuid() {
            DataType::Bytea
        } else {
            DataType::Varchar
        });
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
        let table = load_table_at_least(&self.iceberg_config, expected_snapshot)
            .instrument_await("compaction_resolver_load_table")
            .await?;
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

        // The primary keys deleted during the compaction window, recovered from the input data files.
        // Without a commit after R, nothing was deleted during the window.
        let delete_diff_started = Instant::now();
        let delete_diff_pk_set = if snapshot_r.snapshot_id() == snapshot_n.snapshot_id() {
            HashSet::new()
        } else {
            collect_delete_diff_pks(
                &table,
                snapshot_r,
                snapshot_n,
                &task_input.input_data_file_paths,
                &self.pk_indices,
            )
            .instrument_await("compaction_resolver_collect_delete_diff")
            .await?
        };
        tracing::info!(
            sink_id = %self.sink_id,
            read_snapshot_id = snapshot_r.snapshot_id(),
            current_snapshot_id = snapshot_n.snapshot_id(),
            delete_diff_pks = delete_diff_pk_set.len(),
            elapsed_ms = delete_diff_started.elapsed().as_millis() as u64,
            "compaction resolver collected delete diff"
        );

        let data_file_locator = if self.iceberg_config.pk_index_stores_file_uuid() {
            Some(PkIndexDataFileLocator::new(table.metadata())?)
        } else {
            None
        };
        let scan_started = Instant::now();
        let mut survivor_rows = 0;
        let mut conflicts: HashMap<String, DeleteVector> = HashMap::new();
        #[for_await]
        for chunk in self.scan_output_files(
            file_io,
            data_file_locator.as_ref(),
            &task_input.output_data_file_paths,
            Arc::new(delete_diff_pk_set),
            &mut conflicts,
        ) {
            let chunk = chunk?;
            survivor_rows += chunk.cardinality();
            yield chunk;
        }
        tracing::info!(
            sink_id = %self.sink_id,
            output_files = task_input.output_data_file_paths.len(),
            survivor_rows,
            conflict_files = conflicts.len(),
            elapsed_ms = scan_started.elapsed().as_millis() as u64,
            "compaction resolver scanned output files"
        );

        *conflict_delete_metadata =
            write_conflict_delete_files(&self.iceberg_config, &table, conflicts)
                .instrument_await("compaction_resolver_write_conflict_deletes")
                .await?;
    }

    /// Scans the output data files on spawned tasks, one per row group. Decoding them is
    /// CPU-bound, so scanning them inside the actor would rebuild the whole pk index on a single
    /// core, and scanning a file sequentially would pay the object store latency once per row
    /// group.
    #[try_stream(ok = DataChunk, error = SinkError)]
    async fn scan_output_files<'a>(
        &'a self,
        file_io: &'a FileIO,
        data_file_locator: Option<&'a PkIndexDataFileLocator>,
        output_data_file_paths: &'a [String],
        delete_diff_pk_set: Arc<HashSet<OwnedRow>>,
        conflicts: &'a mut HashMap<String, DeleteVector>,
    ) {
        let splits: Vec<OutputScanSplit> = futures::stream::iter(
            output_data_file_paths
                .iter()
                .map(|path| plan_output_file_splits(file_io, path)),
        )
        .buffered(OUTPUT_FILE_OPEN_CONCURRENCY)
        .try_collect::<Vec<_>>()
        .instrument_await("compaction_resolver_plan_output_scan")
        .await?
        .into_iter()
        .flatten()
        .collect();
        tracing::info!(
            sink_id = %self.sink_id,
            output_files = output_data_file_paths.len(),
            row_groups = splits.len(),
            "compaction resolver planned output scan"
        );

        let pk_indices: Arc<[usize]> = self.pk_indices.as_slice().into();
        let (chunk_tx, mut chunk_rx) = mpsc::channel(OUTPUT_SCAN_CHANNEL_CAPACITY);
        let permits = Arc::new(Semaphore::new(OUTPUT_SCAN_CONCURRENCY));
        let split_paths: Vec<Arc<str>> = splits.iter().map(|split| split.path.clone()).collect();
        let mut tasks = AbortOnDrop(
            splits
                .into_iter()
                .map(|split| {
                    let scan = scan_output_split(
                        file_io.clone(),
                        split,
                        pk_indices.clone(),
                        self.chunk_size,
                        delete_diff_pk_set.clone(),
                        chunk_tx.clone(),
                    );
                    let permits = permits.clone();
                    tokio::spawn(async move {
                        let _permit = permits
                            .acquire_owned()
                            .await
                            .expect("output scan semaphore is never closed");
                        scan.await
                    })
                })
                .collect(),
        );
        // The channel closes once every scan task has finished and dropped its sender.
        drop(chunk_tx);

        let output_data_types = self.output_data_types();
        let mut output_projection = None;
        while let Some(chunk) = chunk_rx
            .recv()
            .instrument_await("compaction_resolver_scan_output_files")
            .await
        {
            let chunk = chunk?;
            if output_projection.is_none() {
                output_projection = Some(OutputTypeProjection::new(
                    chunk.data_types(),
                    &output_data_types,
                )?);
            }
            yield output_projection
                .as_ref()
                .expect("initialized from the first output chunk")
                .execute(chunk)
                .await?;
        }

        for (path, task) in split_paths.iter().zip_eq_fast(tasks.0.iter_mut()) {
            let split_conflicts = task
                .await
                .with_context(|| format!("output scan task for {path} failed"))
                .map_err(SinkError::Iceberg)?;
            if let Some(delete_vector) = split_conflicts {
                *conflicts.entry(path.to_string()).or_default() |= delete_vector;
            }
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
            .instrument_await("compaction_resolver_wait_sink_commit")
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

/// Aborts the spawned tasks when dropped, e.g. when the resolver stream fails or is dropped early.
struct AbortOnDrop<T>(Vec<JoinHandle<T>>);

impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        for task in &self.0 {
            task.abort();
        }
    }
}

/// Recovers the primary keys deleted between snapshots R and N from the input data files.
///
/// Only the input files whose position-delete file changed between R and N are read: an unchanged
/// delete file means no new deletes on that file during the window.
async fn collect_delete_diff_pks(
    table: &Table,
    snapshot_r: &SnapshotRef,
    snapshot_n: &SnapshotRef,
    input_data_file_paths: &[String],
    pk_indices: &[usize],
) -> Result<HashSet<OwnedRow>, SinkError> {
    let file_io = table.file_io();
    let input_paths: HashSet<&str> = input_data_file_paths.iter().map(String::as_str).collect();
    let (deletes_r, deletes_n) = futures::future::try_join(
        collect_input_delete_files(table, snapshot_r, &input_paths),
        collect_input_delete_files(table, snapshot_n, &input_paths),
    )
    .await?;
    let changed = changed_input_delete_files(input_data_file_paths, &deletes_r, &deletes_n);
    tracing::info!(
        input_files = input_data_file_paths.len(),
        delete_files_r = deletes_r.len(),
        delete_files_n = deletes_n.len(),
        changed_delete_files = changed.len(),
        "compaction resolver collecting delete diff"
    );

    let pks: Vec<Vec<OwnedRow>> = futures::stream::iter(changed.into_iter().map(
        |(input_path, delete_r, delete_n)| async move {
            let (positions_n, positions_r) = futures::future::try_join(
                read_position_deletes_from_file(file_io, delete_n),
                async {
                    match delete_r {
                        Some(delete_r) => read_position_deletes_from_file(file_io, delete_r)
                            .await
                            .map(Some),
                        None => Ok(None),
                    }
                },
            )
            .await
            .map_err(SinkError::Iceberg)?;
            let mut diff = positions_n;
            if let Some(positions_r) = &positions_r {
                diff -= positions_r;
            }
            if diff.is_empty() {
                return Ok(Vec::new());
            }
            scan_input_pks_at_positions(file_io, input_path, pk_indices, &diff).await
        },
    ))
    .buffer_unordered(DELETE_DIFF_CONCURRENCY)
    .try_collect()
    .await?;
    Ok(pks.into_iter().flatten().collect())
}

/// Returns `(input path, delete file at R, delete file at N)` for every input data file whose live
/// position-delete file at N differs from the one at R.
fn changed_input_delete_files<'a>(
    input_data_file_paths: &'a [String],
    deletes_r: &'a HashMap<String, DataFile>,
    deletes_n: &'a HashMap<String, DataFile>,
) -> Vec<(&'a str, Option<&'a DataFile>, &'a DataFile)> {
    input_data_file_paths
        .iter()
        .filter_map(|input_path| {
            let delete_n = deletes_n.get(input_path.as_str())?;
            let delete_r = deletes_r.get(input_path.as_str());
            if delete_r.is_some_and(|delete_r| is_same_delete_file(delete_r, delete_n)) {
                return None;
            }
            Some((input_path.as_str(), delete_r, delete_n))
        })
        .collect()
}

/// Delete files are immutable, so the same location identifies the same deleted positions. A
/// deletion vector is a blob inside a Puffin file, so its offset and length are part of its location.
fn is_same_delete_file(a: &DataFile, b: &DataFile) -> bool {
    a.file_path() == b.file_path()
        && a.content_offset() == b.content_offset()
        && a.content_size_in_bytes() == b.content_size_in_bytes()
}

/// Walks the delete manifests of `snapshot` and returns, per input data file, the live
/// position-delete file referencing it.
async fn collect_input_delete_files(
    table: &Table,
    snapshot: &SnapshotRef,
    input_paths: &HashSet<&str>,
) -> Result<HashMap<String, DataFile>, SinkError> {
    let file_io = table.file_io();
    let manifest_list = table
        .object_cache()
        .get_manifest_list(snapshot, &table.metadata_ref())
        .await?;

    // Only delete manifests can carry position-delete files.
    let manifests: Vec<_> = futures::stream::iter(
        manifest_list
            .entries()
            .iter()
            .filter(|manifest_file| manifest_file.content == ManifestContentType::Deletes)
            .map(|manifest_file| manifest_file.load_manifest(file_io)),
    )
    .buffer_unordered(MANIFEST_LOAD_CONCURRENCY)
    .try_collect()
    .await?;

    let mut map: HashMap<String, DataFile> = HashMap::new();
    for manifest in &manifests {
        for entry in manifest.entries() {
            if !entry.is_alive() {
                continue;
            }
            let data_file = entry.data_file();
            if data_file.content_type() == DataContentType::PositionDeletes
                && let Some(referenced) = data_file.referenced_data_file()
                && input_paths.contains(referenced.as_str())
                && map.insert(referenced, data_file.clone()).is_some()
            {
                return Err(SinkError::Iceberg(anyhow!(
                    "input data file {} has multiple live position-delete files in snapshot {}",
                    data_file.referenced_data_file().unwrap(),
                    snapshot.snapshot_id()
                )));
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

type OutputChunkSender = mpsc::Sender<Result<DataChunk, SinkError>>;

/// Like the cast projection after an Iceberg engine-table scan, restores the writer's
/// declared types using SQL casts. Build expressions once and reuse unchanged columns.
/// Delete-diff comparisons remain in physical types on both sides.
struct OutputTypeProjection {
    input_types: Vec<DataType>,
    casts: Vec<Option<BoxedExpression>>,
}

#[expect(
    clippy::disallowed_methods,
    reason = "PK index rebuilds must fail on cast errors instead of substituting NULL keys"
)]
impl OutputTypeProjection {
    fn new(input_types: Vec<DataType>, output_types: &[DataType]) -> Result<Self, SinkError> {
        if input_types.len() != output_types.len() {
            return Err(SinkError::Iceberg(anyhow!(
                "compaction resolver output column count mismatch: expected {}, got {}",
                output_types.len(),
                input_types.len()
            )));
        }
        let casts = input_types
            .iter()
            .zip_eq_fast(output_types)
            .enumerate()
            .map(|(index, (source, target))| {
                if source == target {
                    return Ok(None);
                }
                build_func(
                    PbType::Cast,
                    target.clone(),
                    vec![InputRefExpression::new(source.clone(), index).boxed()],
                )
                .map(Some)
                .with_context(|| {
                    format!("build compaction resolver cast from {source} to {target}")
                })
                .map_err(SinkError::Iceberg)
            })
            .collect::<Result<_, _>>()?;
        Ok(Self { input_types, casts })
    }

    async fn execute(&self, chunk: DataChunk) -> Result<DataChunk, SinkError> {
        if chunk.data_types() != self.input_types {
            return Err(SinkError::Iceberg(anyhow!(
                "compaction resolver output schema changed between chunks"
            )));
        }
        if self.casts.iter().all(Option::is_none) {
            return Ok(chunk);
        }
        let mut columns = Vec::with_capacity(self.casts.len());
        for (cast, column) in self.casts.iter().zip_eq_fast(chunk.columns()) {
            columns.push(match cast {
                Some(cast) => cast
                    .eval(&chunk)
                    .await
                    .context("restore compaction resolver output types")
                    .map_err(SinkError::Iceberg)?,
                None => column.clone(),
            });
        }
        Ok(DataChunk::new(columns, chunk.into_parts_v2().1))
    }
}

/// One row group of an output data file, scanned by its own task.
struct OutputScanSplit {
    path: Arc<str>,
    file_size: u64,
    /// The parquet footer, loaded once per file and shared by all of its splits.
    reader_metadata: ArrowReaderMetadata,
    row_group: usize,
    /// Position of the first row of the row group in the file.
    first_row_position: i64,
}

/// Loads the parquet footer of an output data file and splits the file into its row groups.
async fn plan_output_file_splits(
    file_io: &FileIO,
    path: &str,
) -> Result<Vec<OutputScanSplit>, SinkError> {
    let input_file = file_io.new_input(path)?;
    let file_metadata = input_file.metadata().await?;
    let file_size = file_metadata.size;
    let mut reader = ParquetFileReader::new(file_metadata, input_file.reader().await?);
    let reader_metadata = ArrowReaderMetadata::load_async(&mut reader, Default::default())
        .await
        .map_err(|e| anyhow!(e).context(format!("load parquet metadata of {path}")))?;

    let path: Arc<str> = path.into();
    let mut first_row_position = 0;
    let splits = (reader_metadata.metadata().row_groups().iter().enumerate())
        .map(|(row_group, row_group_metadata)| {
            let split = OutputScanSplit {
                path: path.clone(),
                file_size,
                reader_metadata: reader_metadata.clone(),
                row_group,
                first_row_position,
            };
            first_row_position += row_group_metadata.num_rows();
            split
        })
        .collect();
    Ok(splits)
}

/// Scans one row group of an output data file, sends its survivor rows to `chunk_tx`, and returns
/// the positions of its conflict rows. A scan error is sent to `chunk_tx` too, so the resolver
/// fails without waiting for the other scans.
async fn scan_output_split(
    file_io: FileIO,
    split: OutputScanSplit,
    pk_indices: Arc<[usize]>,
    chunk_size: usize,
    delete_diff_pk_set: Arc<HashSet<OwnedRow>>,
    chunk_tx: OutputChunkSender,
) -> Option<DeleteVector> {
    match scan_output_split_inner(
        &file_io,
        split,
        &pk_indices,
        chunk_size,
        &delete_diff_pk_set,
        &chunk_tx,
    )
    .await
    {
        Ok(conflicts) => conflicts,
        Err(e) => {
            // The receiver is gone only if the resolver already stopped.
            let _ = chunk_tx.send(Err(e)).await;
            None
        }
    }
}

async fn scan_output_split_inner(
    file_io: &FileIO,
    split: OutputScanSplit,
    pk_indices: &[usize],
    chunk_size: usize,
    delete_diff_pk_set: &HashSet<OwnedRow>,
    chunk_tx: &OutputChunkSender,
) -> Result<Option<DeleteVector>, SinkError> {
    let path = &*split.path;
    let reader = ParquetFileReader::new(
        FileMetadata {
            size: split.file_size,
        },
        file_io.new_input(path)?.reader().await?,
    );
    let builder = ParquetRecordBatchStreamBuilder::new_with_metadata(reader, split.reader_metadata);
    let (projection, pk_order) = pk_projection(builder.parquet_schema(), pk_indices)?;
    let mut stream = builder
        .with_projection(projection)
        .with_row_groups(vec![split.row_group])
        .with_batch_size(chunk_size)
        .build()
        .map_err(|e| anyhow!(e).context(format!("build parquet stream for {path}")))?;

    let mut conflicts = DeleteVector::default();
    let mut position = split.first_row_position;
    while let Some(batch) = stream.next().await {
        let batch =
            batch.map_err(|e| anyhow!(e).context(format!("read parquet batch of {path}")))?;
        let pk_chunk = IcebergArrowConvert
            .chunk_from_record_batch(&batch)
            .map_err(|e| anyhow!(e).context(format!("convert parquet batch of {path}")))?
            .project(&pk_order);
        let row_count = pk_chunk.capacity();
        if let Some(chunk) =
            survivor_chunk(pk_chunk, path, position, delete_diff_pk_set, &mut conflicts)
            && chunk_tx.send(Ok(chunk)).await.is_err()
        {
            return Ok(None);
        }
        position += row_count as i64;
    }

    Ok((!conflicts.is_empty()).then_some(conflicts))
}

/// Builds the survivor chunk `[pk_columns.., file_path, position]` of a batch of output rows whose
/// first row is at `first_position` in `path`. Rows whose pk was deleted during the compaction
/// window are recorded in `conflicts` instead. Returns `None` if no row survives.
///
/// The chunk is assembled column by column: the pk columns are reused as-is, so the common case
/// where nothing was deleted never touches individual rows.
fn survivor_chunk(
    pk_chunk: DataChunk,
    path: &str,
    first_position: i64,
    delete_diff_pk_set: &HashSet<OwnedRow>,
    conflicts: &mut DeleteVector,
) -> Option<DataChunk> {
    let row_count = pk_chunk.capacity();
    let positions = first_position..first_position + row_count as i64;
    let visibility: Bitmap = if delete_diff_pk_set.is_empty() {
        row_count.into()
    } else {
        (0..row_count)
            .map(|row_idx| {
                let conflict =
                    delete_diff_pk_set.contains(&pk_chunk.row_at(row_idx).0.to_owned_row());
                if conflict {
                    conflicts.insert((first_position + row_idx as i64) as u64);
                }
                !conflict
            })
            .collect()
    };
    if !visibility.any() {
        return None;
    }

    let (mut columns, _) = pk_chunk.into_parts();
    columns.push(Arc::new(ArrayImpl::Utf8(
        std::iter::repeat_n(path, row_count).collect::<Utf8Array>(),
    )));
    columns.push(Arc::new(ArrayImpl::Int64(positions.collect::<I64Array>())));
    let chunk = DataChunk::new(columns, visibility);
    Some(if chunk.is_vis_compacted() {
        chunk
    } else {
        chunk.compact_vis()
    })
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
    let uuid_suffix = Uuid::now_v7();
    let file_name_generators = PositionDeleteFileNameGenerators::new(uuid_suffix);
    let format_version = table.metadata().format_version();

    let mut delete_files = Vec::with_capacity(conflicts.len());
    for (output_file, delete_vector) in conflicts {
        // Compaction outputs are not committed yet and the resolver only receives their paths,
        // so these delete files intentionally omit `PartitionKey`. Meta backfills each partition
        // from the referenced compactor output file during the compaction pre-commit.
        let file = write_position_delete_file(
            table,
            config,
            &location_generator,
            &file_name_generators,
            format_version,
            output_file,
            &delete_vector,
            None,
        )
        .await?;
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

    use iceberg::spec::{DataFileBuilder, DataFileFormat, Struct};
    use parquet::arrow::ArrowWriter;
    use parquet::basic::Type as PhysicalType;
    use parquet::file::properties::WriterProperties;
    use parquet::schema::types::{SchemaDescriptor, Type};
    use risingwave_common::array::arrow::arrow_schema_58 as arrow_schema;
    use risingwave_common::array::{Array, ArrayBuilderImpl, DataChunkTestExt};
    use risingwave_common::types::{MapType, StructType};
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

    #[test]
    fn test_survivor_chunk_assigns_positions_and_skips_conflicts() {
        let pk_chunk = DataChunk::from_pretty(
            "I
             1
             2
             3",
        );
        let pk = |id: i64| OwnedRow::new(vec![Some(ScalarImpl::Int64(id))]);
        let mut conflicts = DeleteVector::default();

        let chunk =
            survivor_chunk(pk_chunk.clone(), "f", 10, &HashSet::new(), &mut conflicts).unwrap();
        assert_eq!(
            chunk,
            DataChunk::from_pretty(
                "I T I
                 1 f 10
                 2 f 11
                 3 f 12",
            )
        );
        assert!(conflicts.is_empty());

        let deleted = HashSet::from([pk(2)]);
        let chunk = survivor_chunk(pk_chunk.clone(), "f", 10, &deleted, &mut conflicts).unwrap();
        assert_eq!(
            chunk,
            DataChunk::from_pretty(
                "I T I
                 1 f 10
                 3 f 12",
            )
        );
        assert_eq!(conflicts.iter().collect::<Vec<_>>(), vec![11]);

        let all_deleted = HashSet::from([pk(1), pk(2), pk(3)]);
        let mut conflicts = DeleteVector::default();
        assert!(survivor_chunk(pk_chunk, "f", 0, &all_deleted, &mut conflicts).is_none());
        assert_eq!(conflicts.iter().collect::<Vec<_>>(), vec![0, 1, 2]);
    }

    #[tokio::test]
    async fn test_restore_output_types_restores_smallint_keys() {
        let chunk = DataChunk::from_pretty(
            "i T I
             -32768 f 10
             32767 f 11",
        );
        let path_column = chunk.column_at(1).clone();
        let position_column = chunk.column_at(2).clone();
        let chunk = restore_output_types(
            chunk,
            &[DataType::Int16, DataType::Varchar, DataType::Int64],
        )
        .await
        .unwrap();
        assert_eq!(
            chunk.data_types(),
            vec![DataType::Int16, DataType::Varchar, DataType::Int64]
        );
        assert_eq!(
            chunk.column_at(0).as_int16().iter().collect::<Vec<_>>(),
            vec![Some(i16::MIN), Some(i16::MAX)]
        );
        assert!(Arc::ptr_eq(chunk.column_at(1), &path_column));
        assert!(Arc::ptr_eq(chunk.column_at(2), &position_column));
    }

    #[tokio::test]
    async fn test_restore_output_types_rejects_mismatches() {
        for value in [-32769, 32768] {
            let chunk = DataChunk::from_pretty(&format!("i\n{value}"));
            assert!(
                restore_output_types(chunk, &[DataType::Int16])
                    .await
                    .is_err()
            );
        }
        let chunk = DataChunk::from_pretty("i\n1");
        assert!(
            restore_output_types(chunk.clone(), &[DataType::Bytea])
                .await
                .is_err()
        );
        assert!(restore_output_types(chunk.clone(), &[]).await.is_err());
        assert_eq!(
            restore_output_types(chunk.clone(), &[DataType::Int32])
                .await
                .unwrap(),
            chunk
        );
        let projection =
            OutputTypeProjection::new(vec![DataType::Int32], &[DataType::Int16]).unwrap();
        assert!(
            projection
                .execute(DataChunk::from_pretty("T\nx"))
                .await
                .is_err()
        );
    }

    async fn restore_output_types(
        chunk: DataChunk,
        output_types: &[DataType],
    ) -> Result<DataChunk, SinkError> {
        OutputTypeProjection::new(chunk.data_types(), output_types)?
            .execute(chunk)
            .await
    }

    #[tokio::test]
    async fn test_restore_output_types_round_trips_iceberg_type_mappings() {
        for (data_type, text) in [
            (DataType::Int16, "-7"),
            (DataType::Jsonb, r#"{"key": [1, null, true]}"#),
            (DataType::Interval, "2 days 03:04:05"),
            (DataType::Serial, "123"),
            (DataType::list(DataType::Int16), "{-3,7,NULL}"),
            (
                DataType::Struct(StructType::new(vec![
                    ("id", DataType::Int16),
                    ("name", DataType::Varchar),
                ])),
                "(7,hello)",
            ),
            (
                DataType::Map(MapType::from_kv(DataType::Int16, DataType::Int16)),
                "{1:2,3:NULL}",
            ),
        ] {
            let value = ScalarImpl::from_text_for_test(text, &data_type).unwrap();
            let mut builder = ArrayBuilderImpl::with_type(2, data_type.clone());
            builder.append(Some(value));
            builder.append(None::<ScalarImpl>);
            let original = DataChunk::new(vec![Arc::new(builder.finish())], 2);
            let schema = Arc::new(arrow_schema::Schema::new(vec![
                IcebergArrowConvert
                    .to_arrow_field("pk", &data_type)
                    .unwrap(),
            ]));
            let batch = IcebergArrowConvert
                .to_record_batch(schema, &original)
                .unwrap();
            let physical = IcebergArrowConvert.chunk_from_record_batch(&batch).unwrap();
            assert_ne!(physical.data_types(), original.data_types(), "{data_type}");
            assert_eq!(
                restore_output_types(physical, std::slice::from_ref(&data_type))
                    .await
                    .unwrap(),
                original,
                "{data_type}"
            );
        }
    }

    #[cfg_attr(madsim, ignore = "requires Iceberg's native Tokio runtime")]
    #[tokio::test]
    async fn test_output_scan_preserves_positions_across_row_groups() {
        let pk_chunk = DataChunk::from_pretty(
            "i
             1
             2
             3
             4
             5
             6",
        );
        let schema = Arc::new(arrow_schema::Schema::new(vec![
            IcebergArrowConvert
                .to_arrow_field("id", &DataType::Int32)
                .unwrap(),
        ]));
        let batch = IcebergArrowConvert
            .to_record_batch(schema.clone(), &pk_chunk)
            .unwrap();
        let mut bytes = Vec::new();
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(2))
            .build();
        let mut writer = ArrowWriter::try_new(&mut bytes, schema, Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let file_io = FileIO::new_with_memory();
        let path = "memory:/output.parquet";
        file_io
            .new_output(path)
            .unwrap()
            .write(bytes.into())
            .await
            .unwrap();
        let splits = plan_output_file_splits(&file_io, path).await.unwrap();
        assert_eq!(
            splits
                .iter()
                .map(|s| s.first_row_position)
                .collect::<Vec<_>>(),
            vec![0, 2, 4]
        );

        // Conflicts in both later row groups must use file-relative positions, even when
        // splits finish out of order and each row group spans multiple batches.
        let deleted = Arc::new(HashSet::from([
            OwnedRow::new(vec![Some(ScalarImpl::Int32(4))]),
            OwnedRow::new(vec![Some(ScalarImpl::Int32(5))]),
        ]));
        let (tx, mut rx) = mpsc::channel(OUTPUT_SCAN_CHANNEL_CAPACITY);
        let mut conflicts = DeleteVector::default();
        for split in splits.into_iter().rev() {
            if let Some(dv) = scan_output_split(
                file_io.clone(),
                split,
                Arc::from([0]),
                1,
                deleted.clone(),
                tx.clone(),
            )
            .await
            {
                conflicts |= dv;
            }
        }
        drop(tx);
        let mut survivors = Vec::new();
        while let Some(chunk) = rx.recv().await {
            let chunk = restore_output_types(
                chunk.unwrap(),
                &[DataType::Int16, DataType::Varchar, DataType::Int64],
            )
            .await
            .unwrap();
            for row in chunk.rows() {
                survivors.push((
                    row.datum_at(0).unwrap().into_int16(),
                    row.datum_at(2).unwrap().into_int64(),
                ));
                assert_eq!(row.datum_at(1).unwrap().into_utf8(), path);
            }
        }
        survivors.sort_unstable();
        assert_eq!(survivors, vec![(1, 0), (2, 1), (3, 2), (6, 5)]);
        assert_eq!(conflicts.iter().collect::<Vec<_>>(), vec![3, 4]);
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

    fn delete_file(path: &str, referenced: &str, content_offset: i64) -> DataFile {
        DataFileBuilder::default()
            .content(DataContentType::PositionDeletes)
            .file_path(path.to_owned())
            .file_format(DataFileFormat::Puffin)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition(Struct::empty())
            .referenced_data_file(Some(referenced.to_owned()))
            .content_offset(Some(content_offset))
            .content_size_in_bytes(Some(10))
            .build()
            .unwrap()
    }

    #[test]
    fn test_changed_input_delete_files_skips_unchanged_delete_files() {
        let input_paths: Vec<String> = ["unchanged", "new_blob", "new_dv", "only_r", "no_dv"]
            .into_iter()
            .map(String::from)
            .collect();
        let deletes_r = HashMap::from([
            (
                "unchanged".to_owned(),
                delete_file("r.puffin", "unchanged", 4),
            ),
            (
                "new_blob".to_owned(),
                delete_file("r.puffin", "new_blob", 14),
            ),
            ("only_r".to_owned(), delete_file("r.puffin", "only_r", 24)),
        ]);
        let deletes_n = HashMap::from([
            (
                "unchanged".to_owned(),
                delete_file("r.puffin", "unchanged", 4),
            ),
            // Same Puffin file, but a different blob in it.
            (
                "new_blob".to_owned(),
                delete_file("r.puffin", "new_blob", 34),
            ),
            ("new_dv".to_owned(), delete_file("n.puffin", "new_dv", 4)),
        ]);

        let changed: Vec<_> = changed_input_delete_files(&input_paths, &deletes_r, &deletes_n)
            .into_iter()
            .map(|(path, delete_r, delete_n)| {
                (
                    path,
                    delete_r.map(|file| file.content_offset()),
                    delete_n.content_offset(),
                )
            })
            .collect();
        assert_eq!(
            changed,
            vec![
                ("new_blob", Some(Some(14)), Some(34)),
                ("new_dv", None, Some(4)),
            ]
        );
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
