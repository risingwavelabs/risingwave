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

//! How the sink executor compacts changes.
//!
//! Changes are compacted on the writer side, before the log store, and again on the reader side,
//! after it. The reader merges what the writer wrote, so both sides are decided together here,
//! once for each executor, from the sink and its input.

use risingwave_common::array::StreamChunk;
use risingwave_common::types::DataType;
use risingwave_connector::sink::catalog::SinkType;
use risingwave_pb::stream_plan::stream_node::StreamKind;

use crate::common::change_buffer::{OutputKind, output_kind};
use crate::common::compact_chunk::{
    InconsistencyBehavior, StreamChunkCompactor, compact_chunk_inline,
};

/// Dispatches `$body` on the runtime output kind `$output`, with `$KIND` as a constant.
macro_rules! with_output_kind {
    ($output:expr, $KIND:ident, $body:expr) => {
        match $output {
            output_kind::UPSERT => {
                const $KIND: OutputKind = output_kind::UPSERT;
                $body
            }
            output_kind::RETRACT => {
                const $KIND: OutputKind = output_kind::RETRACT;
                $body
            }
        }
    };
}

/// Merges the changes of each `key` and emits them in `output` form.
#[derive(Clone, Debug)]
pub(super) struct Compaction {
    pub key: Vec<usize>,
    /// What the merged changes mean. Upsert changes keep the last operation of each key, while
    /// retract changes also cancel an insert followed by a delete of the same key.
    pub input: StreamKind,
    pub output: OutputKind,
    pub ib: InconsistencyBehavior,
}

impl Compaction {
    pub fn compact_chunk(&self, chunk: StreamChunk) -> StreamChunk {
        with_output_kind!(
            self.output,
            KIND,
            compact_chunk_inline::<KIND>(chunk, &self.key, self.input, self.ib)
        )
    }

    pub fn compact_inline(&self, chunks: Vec<StreamChunk>) -> Vec<StreamChunk> {
        let compactor = StreamChunkCompactor::new(self.key.clone(), chunks, self.input);
        with_output_kind!(
            self.output,
            KIND,
            compactor.into_compacted_chunks_inline::<KIND>(self.ib)
        )
    }

    pub fn compact_reconstructed(
        &self,
        chunks: Vec<StreamChunk>,
        chunk_size: usize,
        data_types: Vec<DataType>,
    ) -> Vec<StreamChunk> {
        let compactor = StreamChunkCompactor::new(self.key.clone(), chunks, self.input);
        with_output_kind!(
            self.output,
            KIND,
            compactor.into_compacted_chunks_reconstructed::<KIND>(chunk_size, data_types, self.ib)
        )
    }

    /// What the emitted changes mean. Upsert output drops the old row of an update, so a later
    /// insert of the same key may overwrite a row the consumer has.
    fn emits(&self) -> StreamKind {
        if self.input == StreamKind::Upsert || self.output == output_kind::UPSERT {
            StreamKind::Upsert
        } else {
            StreamKind::Retract
        }
    }
}

#[derive(Clone, Debug)]
pub(super) enum WriterCompaction {
    /// Forward the changes as they are.
    None,
    /// Compact each chunk.
    PerChunk(Compaction),
    /// Buffer each epoch, compact it by the stream key and emit all deletes before all inserts,
    /// then finish it as `then` says.
    ///
    /// This is needed when the stream key does not determine the downstream pk, as the changes may
    /// then be out of order. For example, take a stream with stream key `a, b` and downstream pk
    /// `a`, holding `(1, 1)`. Two `UPDATE`s issue:
    ///
    /// ```text
    /// UPDATE SET b = 2 WHERE a = 1 ... which issues:
    ///   - (1, 1)
    ///   + (1, 2)
    ///
    /// UPDATE SET b = 3 WHERE a = 1 ... which issues:
    ///   - (1, 2)
    ///   + (1, 3)
    /// ```
    ///
    /// These changes may be shuffled to different actors, as they are under different stream keys,
    /// and merged back into the sink actor in any order, like:
    ///
    /// ```text
    /// + (1, 2) -- Actor 2, first row
    /// + (1, 3) -- Actor 3
    /// - (1, 1) -- Actor 1
    /// - (1, 2) -- Actor 2, second row
    /// ```
    ///
    /// In terms of the stream key, this order is correct, since it operates on 3 different rows.
    /// In terms of the downstream pk `a`, it violates the unique constraint all the time. So the
    /// changes of each epoch are compacted by the stream key first:
    ///
    /// ```text
    /// + (1, 3)
    /// - (1, 1)
    /// ```
    ///
    /// Then all deletes are emitted before all inserts:
    ///
    /// ```text
    /// - (1, 1)
    /// + (1, 3)
    /// ```
    ///
    /// After the compaction, each stream key has at most one delete and one insert, so the deletes
    /// can be moved to the front, after which the inserts keep the downstream pk unique.
    ///
    /// With `force_compaction`, this is also done when the stream key determines the downstream pk,
    /// to reduce the output and emit at most one change for each key in an epoch.
    PerEpoch {
        by_stream_key: Compaction,
        then: EpochFinish,
    },
}

#[derive(Clone, Debug)]
pub(super) enum EpochFinish {
    /// Emit the reordered changes.
    None,
    /// Compact the reordered changes again by the downstream pk, which also turns a delete and an
    /// insert of the same pk into an update.
    ByDownstreamPk(Compaction),
    /// Drop the deletes of downstream pks inserted again in the epoch, since such a pk is updated
    /// rather than deleted from the downstream's perspective. With `preserve_row_level_changes`,
    /// the downstream must see the change of each stream key, so the changes are not compacted
    /// again by the downstream pk.
    DropReinsertedDeletes { downstream_pk: Vec<usize> },
}

/// The configuration of a sink executor that compaction depends on.
#[derive(Clone, Copy)]
pub(super) struct CompactionConfig<'a> {
    pub sink_type: SinkType,
    pub input_kind: StreamKind,
    pub stream_key: &'a [usize],
    pub downstream_pk: Option<&'a [usize]>,
    pub force_compaction: bool,
    pub preserve_row_level_changes: bool,
    /// Skip writer-side compaction when the stream key is the downstream pk, for benchmarks with
    /// the blackhole sink.
    pub skip_writer_compaction: bool,
    pub ignore_delete: bool,
    /// False for a sink into table, which bypasses the log store.
    pub has_log_store: bool,
}

#[derive(Clone, Debug)]
pub(super) struct CompactionPlan {
    /// Drop deletes and turn updates into inserts before any compaction. Compaction assumes that
    /// deletes take effect, so it would otherwise merge an insert or update into a later delete
    /// that is then dropped.
    pub ignore_delete: bool,
    /// The compaction before the log store, of the changes left after `ignore_delete`.
    pub writer: WriterCompaction,
    /// Compaction of each chunk read from the log store, whose `input` is what the writer emits.
    ///
    /// JDBC sinks and the Postgres sink execute the deletes of a chunk before its inserts, so a
    /// chunk must have at most one change for each key (#12250). The log store does not keep
    /// chunk boundaries, so a chunk read may combine several chunks written to it, and needs to
    /// be compacted again. A sink into table does not use the log store. An epoch compacted as a
    /// whole already has this property when compacted again by the downstream pk; otherwise, it
    /// either has no downstream pk to compact by, or has `preserve_row_level_changes`, which the
    /// planner only sets for a sink into table.
    pub reader: Option<Compaction>,
}

impl CompactionPlan {
    pub fn new(config: &CompactionConfig<'_>) -> Self {
        let CompactionConfig {
            sink_type,
            input_kind,
            stream_key,
            downstream_pk,
            force_compaction,
            preserve_row_level_changes,
            skip_writer_compaction,
            ignore_delete,
            has_log_store,
        } = *config;
        let nothing = Self {
            ignore_delete,
            writer: WriterCompaction::None,
            reader: None,
        };
        let output = match sink_type {
            SinkType::AppendOnly => return nothing,
            SinkType::Upsert => output_kind::UPSERT,
            SinkType::Retract => output_kind::RETRACT,
        };
        // Without deletes, a later version of a stream key no longer removes the row of the version
        // before: every inserted row stays in the downstream until an insert of the same downstream
        // pk overwrites it, as in an upsert stream keyed by the downstream pk.
        let (stream_key, input) = if ignore_delete {
            (downstream_pk.unwrap_or(stream_key), StreamKind::Upsert)
        } else {
            (stream_key, input_kind)
        };
        // `preserve_row_level_changes` forbids compacting by the downstream pk, which is what
        // `force_compaction` would compact by here, and there are no deletes to move first.
        let force_compaction = force_compaction && !(ignore_delete && preserve_row_level_changes);
        // A user-specified pk may not be unique, so compacting by it never panics on inconsistency.
        let by_pk = |pk: &[usize], input| Compaction {
            key: pk.to_vec(),
            input,
            output,
            ib: InconsistencyBehavior::Warn,
        };

        match downstream_pk {
            // The downstream pk contains the stream key, so the changes of a downstream pk all have
            // the same stream key and arrive in order.
            Some(pk) if stream_key.iter().all(|i| pk.contains(i)) && !force_compaction => {
                let writer = if preserve_row_level_changes {
                    // The target table applies its own conflict semantics to every change.
                    WriterCompaction::None
                } else if skip_writer_compaction && stream_key == pk {
                    // Skipping is only valid when the keys are exactly the same, not just matching
                    // by being a subset.
                    WriterCompaction::None
                } else {
                    WriterCompaction::PerChunk(by_pk(pk, input))
                };
                let written = match &writer {
                    WriterCompaction::PerChunk(compaction) => compaction.emits(),
                    _ => input,
                };
                Self {
                    ignore_delete,
                    writer,
                    reader: has_log_store.then(|| by_pk(pk, written)),
                }
            }
            _ => {
                // With a downstream pk, keep the old row of each update: an update may move a row to
                // another downstream pk, and the old one must still be deleted. Changes on the same
                // downstream pk are folded by `then`. Upsert input is excluded, as its old rows only
                // guarantee the stream key columns, so the old downstream pk is unknown.
                let keep_old_rows = downstream_pk.is_some() && input != StreamKind::Upsert;
                let by_stream_key = Compaction {
                    key: stream_key.to_vec(),
                    input,
                    output: if keep_old_rows {
                        output_kind::RETRACT
                    } else {
                        output
                    },
                    ib: InconsistencyBehavior::Panic,
                };
                let then = match downstream_pk {
                    Some(pk) if !preserve_row_level_changes => {
                        EpochFinish::ByDownstreamPk(by_pk(pk, by_stream_key.emits()))
                    }
                    Some(pk) if output == output_kind::UPSERT => {
                        EpochFinish::DropReinsertedDeletes {
                            downstream_pk: pk.to_vec(),
                        }
                    }
                    _ => EpochFinish::None,
                };
                Self {
                    ignore_delete,
                    writer: WriterCompaction::PerEpoch {
                        by_stream_key,
                        then,
                    },
                    reader: None,
                }
            }
        }
    }
}
