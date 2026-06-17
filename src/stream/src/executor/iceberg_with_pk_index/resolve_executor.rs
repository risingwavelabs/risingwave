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

//! Leaf executor of the transient "resolve" pipeline spun up per Iceberg-V3 compaction.
//!
//! It scans the freshly-rewritten OUTPUT data files via [`scan_output_pks`] and emits candidate
//! rows `[pk_col_0 .. pk_col_n, output_file: Varchar, output_pos: Int64]` (all `Op::Insert`).
//! A downstream hash-exchange routes each row to the writer actor that owns that PK in its index,
//! which then remaps `(output_file, output_pos)` for the still-live PKs.

use anyhow::anyhow;
use either::Either;
use futures::TryStreamExt;
use futures::stream::{self, PollNext, select_with_strategy};
use iceberg::io::FileIO;
use iceberg::spec::{DataFile, SchemaRef, SerializedDataFile};
use iceberg::table::Table;
use risingwave_common::array::Op;
use risingwave_common::bitmap::Bitmap;
use risingwave_common::hash::VirtualNode;
use risingwave_common::id::SinkId;
use risingwave_pb::stream_service::PbIcebergV3SinkRole;
use tokio::sync::mpsc::UnboundedReceiver;

use super::output_scan::{pk_field_ids_by_name, scan_output_pks};
use crate::executor::prelude::*;
use crate::executor::source::barrier_to_message_stream;
use crate::task::LocalBarrierManager;

/// Parameters baked into the plan node for one compaction's resolve scan.
///
/// The iceberg `Table` is loaded up front (in `from_proto`) and passed in, mirroring the writer's
/// table-loading path, so that the data types of the PK columns are known when building chunks.
pub struct ResolveExecutor {
    actor_ctx: ActorContextRef,
    sink_id: SinkId,
    /// The loaded iceberg table providing `file_io` and the current schema for the scan.
    table: Table,
    /// Compaction OUTPUT files (already materialized from `SerializedDataFile`s) to scan in order.
    output_files: Vec<DataFile>,
    /// PK column names in index-key order; resolved to iceberg field-ids to project the scan.
    pk_column_names: Vec<String>,
    /// This actor's vnode ownership, used to select which OUTPUT FILES this actor scans (NOT which
    /// rows it emits). The resolve fragment mirrors the writer's hash distribution, so the per-actor
    /// bitmaps partition the vnode space. Each output file is assigned to one actor by hashing its
    /// file path to a vnode, so every file is scanned exactly once across the fragment. An owning
    /// actor emits EVERY PK row of its files; the downstream Hash dispatcher (keyed on the leading PK
    /// columns) routes each row to the writer actor that owns the matching pk-index entry. `None`
    /// means a singleton actor that owns all vnodes and thus scans every file.
    vnodes: Option<Arc<Bitmap>>,
    /// Barrier channel: this is a leaf, so barriers come straight from the local barrier manager.
    barrier_receiver: Option<UnboundedReceiver<Barrier>>,
    /// Used to report the "scan done" signal (role + epoch only) back to meta on completion.
    local_barrier_manager: LocalBarrierManager,
    chunk_size: usize,
}

impl ResolveExecutor {
    #[expect(clippy::too_many_arguments)]
    pub fn new(
        actor_ctx: ActorContextRef,
        sink_id: SinkId,
        table: Table,
        output_files: Vec<DataFile>,
        pk_column_names: Vec<String>,
        vnodes: Option<Arc<Bitmap>>,
        barrier_receiver: UnboundedReceiver<Barrier>,
        local_barrier_manager: LocalBarrierManager,
        chunk_size: usize,
    ) -> Self {
        Self {
            actor_ctx,
            sink_id,
            table,
            output_files,
            pk_column_names,
            vnodes,
            barrier_receiver: Some(barrier_receiver),
            local_barrier_manager,
            chunk_size,
        }
    }

    /// Decode the serde_json-encoded `Vec<SerializedDataFile>` and materialize each one into a
    /// `DataFile` against the table's default (unpartitioned) spec/type, exactly like the meta-side
    /// coordinator does. The resolver only runs on unpartitioned output, so the default spec applies.
    pub fn decode_output_files(
        table: &Table,
        output_files_bytes: &[u8],
    ) -> StreamExecutorResult<Vec<DataFile>> {
        let serialized: Vec<SerializedDataFile> = serde_json::from_slice(output_files_bytes)
            .map_err(|e| anyhow!(e).context("decode v3 resolve output_files"))?;
        let schema = table.metadata().current_schema();
        let partition_spec_id = table.metadata().default_partition_spec_id();
        let partition_type = table.metadata().default_partition_type();
        serialized
            .into_iter()
            .map(|f| {
                f.try_into(partition_spec_id, partition_type, schema)
                    .map_err(|e| {
                        anyhow!(e)
                            .context("materialize v3 resolve SerializedDataFile")
                            .into()
                    })
            })
            .collect()
    }

    /// Build the data types of the emitted candidate row: the PK columns (in `pk_column_names`
    /// order) followed by the appended `output_file: Varchar` and `output_pos: Int64`.
    ///
    /// The PK column types are derived from the iceberg schema via arrow, matching the dtypes that
    /// [`scan_output_pks`] yields (it converts arrow record batches through `IcebergArrowConvert`).
    /// Arrow fields preserve the iceberg field name, so we look them up by the PK column names
    /// (which were already validated against the schema by `pk_field_ids_by_name`).
    fn output_data_types(
        schema: &SchemaRef,
        pk_column_names: &[String],
    ) -> StreamExecutorResult<Vec<DataType>> {
        use risingwave_common::array::arrow::IcebergArrowConvert;

        let arrow_schema = iceberg::arrow::schema_to_arrow_schema(schema.as_ref())
            .map_err(|e| anyhow!(e).context("iceberg schema -> arrow in v3 resolve"))?;
        let mut data_types = Vec::with_capacity(pk_column_names.len() + 2);
        for name in pk_column_names {
            let arrow_field = arrow_schema.field_with_name(name).map_err(|e| {
                anyhow!(e).context(format!(
                    "v3 resolve: PK column {name:?} not in arrow schema"
                ))
            })?;
            let dt = IcebergArrowConvert
                .type_from_field(arrow_field)
                .map_err(|e| anyhow!(e).context("arrow field -> rw type in v3 resolve"))?;
            data_types.push(dt);
        }
        data_types.push(DataType::Varchar);
        data_types.push(DataType::Int64);
        Ok(data_types)
    }

    /// The data half of the leaf: scan the output files this actor OWNS, in order, and yield every
    /// scanned PK row as a candidate-row chunk. Once all owned files are scanned this stream ends;
    /// the barrier arm then keeps the executor alive (idle, only barriers) until the transient
    /// pipeline is torn down by a later task.
    ///
    /// File-level ownership: the resolve fragment mirrors the writer's hash distribution (N actors
    /// with disjoint vnode bitmaps that partition the vnode space), so each output file is assigned
    /// to exactly ONE actor by hashing its file path to a vnode. The actor scans only the files it
    /// owns and emits EVERY row from them — no per-row filtering. Routing each emitted row to the
    /// writer that owns the PK is the job of the downstream Hash dispatcher (keyed on the leading PK
    /// columns), so each output file is scanned once and each PK is emitted once.
    #[try_stream(ok = StreamChunk, error = StreamExecutorError)]
    async fn scan_stream(
        file_io: FileIO,
        schema: SchemaRef,
        output_files: Vec<DataFile>,
        pk_field_ids: Vec<i32>,
        data_types: Vec<DataType>,
        vnodes: Option<Arc<Bitmap>>,
        chunk_size: usize,
    ) {
        let mut chunk_builder = StreamChunkBuilder::new(chunk_size, data_types);
        for output_file in output_files {
            let file_path = output_file.file_path().to_owned();
            // Assign this file to exactly one actor by hashing its path to a vnode and checking
            // ownership. The per-actor vnode bitmaps partition the vnode space, so every file is
            // owned by exactly one actor and is therefore scanned exactly once across the fragment.
            // A singleton actor (`None`) owns all vnodes and so scans every file. `compute_row` is
            // assertion-free (unlike `compute_vnode`, which panics on a non-owned vnode), so this
            // actor can test ownership itself and skip files it does not own.
            if let Some(vnodes) = &vnodes {
                let file_path_row =
                    OwnedRow::new(vec![Some(ScalarImpl::Utf8(file_path.as_str().into()))]);
                let vnode = VirtualNode::compute_row(&file_path_row, &[0], vnodes.len());
                if !vnodes.is_set(vnode.to_index()) {
                    continue;
                }
            }
            let pk_stream = scan_output_pks(
                file_io.clone(),
                schema.clone(),
                output_file,
                pk_field_ids.clone(),
            );
            #[for_await]
            for item in pk_stream {
                let (pk_row, pos) = item?;
                // This actor owns the file, so emit every scanned PK row. The downstream Hash
                // dispatcher routes each row to the writer actor owning that PK's index entry.
                if let Some(chunk) = chunk_builder
                    .append_row(Op::Insert, build_candidate_row(pk_row, &file_path, pos))
                {
                    yield chunk;
                }
            }
        }
        if let Some(chunk) = chunk_builder.take() {
            yield chunk;
        }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn into_stream(mut self) {
        let mut barrier_receiver = self.barrier_receiver.take().unwrap();
        let first_barrier = barrier_receiver.recv().await.ok_or_else(|| {
            anyhow!(
                "failed to receive the first barrier, actor_id: {:?}, sink_id: {:?}",
                self.actor_ctx.id,
                self.sink_id,
            )
        })?;
        tracing::info!(
            target: "iceberg_v3_resolve",
            actor_id = %self.actor_ctx.id,
            sink_id = %self.sink_id,
            epoch = ?first_barrier.epoch,
            output_files = self.output_files.len(),
            "resolve executor received first barrier, emitting it downstream"
        );
        yield Message::Barrier(first_barrier);

        let schema = self.table.metadata().current_schema().clone();
        let file_io = self.table.file_io().clone();
        let pk_field_ids = pk_field_ids_by_name(&schema, &self.pk_column_names, self.sink_id)?;
        let data_types = Self::output_data_types(&schema, &self.pk_column_names)?;

        // Wrap each scanned chunk as `Some(chunk)` and append a single `None` sentinel so the merge
        // loop can observe scan exhaustion. `select_with_strategy` otherwise gives no signal when
        // one branch ends (the barrier branch keeps yielding), so this sentinel is how we learn the
        // scan is done and can report it on the next checkpoint.
        let scan_stream = Self::scan_stream(
            file_io,
            schema,
            self.output_files,
            pk_field_ids,
            data_types,
            self.vnodes.clone(),
            self.chunk_size,
        )
        .map_ok(Some)
        .chain(stream::once(async { Ok(None) }))
        .map_ok(Either::Right);
        let barrier_stream = barrier_to_message_stream(barrier_receiver).map_ok(Either::Left);

        // Bias toward barriers (Left) so the transient pipeline stays responsive to checkpoints
        // even while a large output file is being scanned.
        let mut merged = select_with_strategy(
            barrier_stream.boxed(),
            scan_stream.boxed(),
            |_: &mut PollNext| PollNext::Left,
        );

        // Once the scan is exhausted we report "done" exactly once on the next checkpoint barrier:
        // this role+epoch signal is what meta uses to detach the transient pipeline and commit.
        let mut scan_done = false;
        let mut done_reported = false;

        while let Some(msg) = merged.next().await {
            match msg? {
                Either::Left(Message::Barrier(barrier)) => {
                    // The scan emitted all candidates and (after detach) the writer's remap Merge
                    // has consumed them; report once on the first checkpoint that follows.
                    if scan_done && !done_reported && barrier.is_checkpoint() {
                        tracing::info!(
                            target: "iceberg_v3_resolve",
                            actor_id = %self.actor_ctx.id,
                            sink_id = %self.sink_id,
                            epoch = ?barrier.epoch,
                            "resolve executor scan exhausted; reporting Resolver-done to meta"
                        );
                        self.local_barrier_manager.report_iceberg_v3_sink_metadata(
                            barrier.epoch,
                            self.sink_id,
                            self.actor_ctx.id,
                            PbIcebergV3SinkRole::Resolver,
                            // The signal is just role+epoch; the resolver has no commit metadata.
                            None,
                        );
                        done_reported = true;
                    }
                    yield Message::Barrier(barrier);
                }
                // Non-barrier messages from the barrier branch (e.g. watermarks) pass through.
                Either::Left(msg) => yield msg,
                Either::Right(Some(chunk)) => yield Message::Chunk(chunk),
                // The sentinel: the candidate scan is exhausted.
                Either::Right(None) => scan_done = true,
            }
        }
    }
}

/// Assemble one emitted candidate row: the scanned PK columns followed by the output file path and
/// the row's dense position. The dtype order here MUST match [`ResolveExecutor::output_data_types`].
fn build_candidate_row(pk_row: OwnedRow, file_path: &str, pos: i64) -> OwnedRow {
    let mut datums: Vec<Datum> = pk_row.into_inner().into_vec();
    datums.push(Some(ScalarImpl::Utf8(file_path.into())));
    datums.push(Some(ScalarImpl::Int64(pos)));
    OwnedRow::new(datums)
}

impl Execute for ResolveExecutor {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.into_stream().boxed()
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::row::Row as _;

    use super::*;

    // The chunk-assembly (PK datums + appended file/pos columns, in the exact dtype order the
    // builder expects) is the one easily-broken pure bit; the IO/scan path is validated live.
    #[test]
    fn test_build_candidate_row_appends_file_and_pos() {
        let pk_row = OwnedRow::new(vec![
            Some(ScalarImpl::Int32(7)),
            Some(ScalarImpl::Utf8("alice".into())),
        ]);
        let row = build_candidate_row(pk_row, "out/file-1.parquet", 42);

        assert_eq!(row.len(), 4);
        assert_eq!(row.datum_at(0), Some(ScalarRefImpl::Int32(7)));
        assert_eq!(row.datum_at(1), Some(ScalarRefImpl::Utf8("alice")));
        assert_eq!(
            row.datum_at(2),
            Some(ScalarRefImpl::Utf8("out/file-1.parquet"))
        );
        assert_eq!(row.datum_at(3), Some(ScalarRefImpl::Int64(42)));
    }
}
