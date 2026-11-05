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
    InconsistencyBehavior, StreamChunkCompactor, compact_chunk_inline, keep_last_change_inline,
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

/// Keeps the last change of each `key` in a chunk read from the log store, in `output` form.
///
/// After a recovery, the sink may have applied the first changes of a chunk already, in chunks
/// cut elsewhere. So an insert and a later delete of a key do not cancel, as the insert may be in
/// the downstream: the delete is kept.
#[derive(Clone, Debug)]
pub(super) struct ReaderCompaction {
    pub key: Vec<usize>,
    pub output: OutputKind,
}

impl ReaderCompaction {
    pub fn compact_chunk(&self, chunk: StreamChunk) -> StreamChunk {
        keep_last_change_inline(chunk, &self.key, self.output)
    }
}

#[derive(Clone, Debug)]
pub(super) enum WriterCompaction {
    /// Forward the changes as they are.
    None,
    /// Forward the changes, with each update that changes `key` as a delete and an insert. A
    /// target table that handles conflicts itself skips the old row of an update, and would keep
    /// the row of the old key.
    SplitUpdates { key: Vec<usize> },
    /// Compact each chunk.
    PerChunk(Compaction),
    /// Buffer each epoch and compact it as a whole.
    PerEpoch(EpochCompaction),
}

#[derive(Clone, Debug)]
pub(super) enum EpochCompaction {
    /// Compact the epoch once, as [`WriterCompaction::PerChunk`] compacts a chunk, for
    /// `force_compaction`: this reduces the output and emits at most one change for each key in an
    /// epoch.
    Once(Compaction),
    /// Compact the epoch by the stream key and emit all deletes before all inserts, then finish it
    /// as `then` says. `by_stream_key` merges the changes of a stream key within each downstream pk.
    ///
    /// This is needed when the downstream pk does not contain the stream key, or is unknown, as the
    /// changes of a downstream pk may then come from several stream keys, out of order. For
    /// example, take a stream with stream key `a, b` and downstream pk `a`, holding `(1, 1)`. Two
    /// `UPDATE`s issue:
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
    Reorder {
        by_stream_key: Compaction,
        then: EpochFinish,
    },
    /// Emit the inserts of the rows gone at the barrier, then those of the rows still there, and
    /// keep the last insert of each downstream pk with `then`. This is for `ignore_delete` when
    /// the downstream pk does not contain the stream key.
    ///
    /// The inserts of a downstream pk then come from several stream keys, in no order, so a row
    /// deleted in the epoch may arrive after the row that replaced it. The deletes are not sent,
    /// but they tell which rows are gone: a key of `live_by` whose last change is an insert still
    /// has that row at the barrier.
    LiveRowsLast {
        live_by: Compaction,
        then: Option<Compaction>,
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
    /// The partition column of a partitioned iceberg table, which the sink is distributed by
    /// instead of the downstream pk.
    pub partition_col: Option<usize>,
}

#[derive(Clone, Debug)]
pub(super) struct CompactionPlan {
    /// Drop deletes and turn updates into inserts before any compaction, for `ignore_delete`.
    /// Compaction assumes that deletes take effect, so it would otherwise merge an insert or
    /// update into a later delete that is then dropped. [`EpochCompaction::LiveRowsLast`] needs
    /// the deletes, and drops them itself.
    pub drop_deletes_first: bool,
    /// The compaction before the log store, of the changes left after `drop_deletes_first`.
    pub writer: WriterCompaction,
    /// Compaction of each chunk read from the log store.
    ///
    /// The JDBC sink executes the deletes of a chunk before its inserts, so a chunk must have at
    /// most one change for each downstream pk (#12250). The log store does not keep chunk
    /// boundaries, so a chunk read may combine several chunks written to it. A sink into table
    /// does not use the log store. An epoch compacted as a whole has at most one change for each
    /// downstream pk, or the changes of each downstream pk in order with its delete first.
    pub reader: Option<ReaderCompaction>,
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
            partition_col,
        } = *config;
        let output = match sink_type {
            SinkType::AppendOnly => {
                return Self {
                    drop_deletes_first: ignore_delete,
                    writer: WriterCompaction::None,
                    reader: None,
                };
            }
            SinkType::Upsert => output_kind::UPSERT,
            SinkType::Retract => output_kind::RETRACT,
        };
        let union = |a: &[usize], b: &[usize]| -> Vec<usize> {
            (a.iter().chain(b.iter().filter(|i| !a.contains(i))).copied()).collect()
        };
        // A log store keeps the order of rows, and pairs the two rows of an update, only within
        // a vnode, so the keys that changes are merged by have to contain the distribution key of
        // the sink. That is made of columns of the downstream pk, or of the stream key without
        // one, except for a partitioned iceberg table. Its deletes only apply within a partition,
        // so the partition column is part of what tells its rows apart.
        let downstream_pk = downstream_pk.map(|pk| union(pk, partition_col.as_slice()));
        let contains_stream_key = |pk: &[usize]| stream_key.iter().all(|i| pk.contains(i));
        // A user-specified pk may not be unique, so compacting by one never panics on inconsistency.
        let compaction_by = |key: &[usize], input| Compaction {
            key: key.to_vec(),
            input,
            output,
            ib: InconsistencyBehavior::Warn,
        };

        if ignore_delete
            && let Some(pk) = &downstream_pk
            && !contains_stream_key(pk)
        {
            let live_by = Compaction {
                key: union(stream_key, pk),
                input: StreamKind::Upsert,
                output,
                ib: InconsistencyBehavior::Panic,
            };
            // With `preserve_row_level_changes`, the target table gets every insert.
            let then = (!preserve_row_level_changes).then(|| compaction_by(pk, StreamKind::Upsert));
            return Self {
                drop_deletes_first: false,
                writer: WriterCompaction::PerEpoch(EpochCompaction::LiveRowsLast { live_by, then }),
                reader: None,
            };
        }
        // Without deletes, a later version of a stream key no longer removes the row of the version
        // before: every inserted row stays in the downstream until an insert of the same downstream
        // pk overwrites it, as in an upsert stream keyed by the downstream pk. Here its inserts
        // are in order, as it contains the stream key.
        let (stream_key, input) = if ignore_delete {
            (
                downstream_pk.as_deref().unwrap_or(stream_key),
                StreamKind::Upsert,
            )
        } else {
            (stream_key, input_kind)
        };

        match downstream_pk.as_deref() {
            // The downstream pk contains the stream key, so the changes of a downstream pk all have
            // the same stream key and arrive in order. One compaction then leaves at most one change
            // for each downstream pk, if it merges by a key within the downstream pk without losing
            // changes: the downstream pk itself for retract changes, which carry their old rows, and
            // the stream key for upsert changes, which replace the row of their stream key without
            // carrying it.
            Some(pk) if stream_key.iter().all(|i| pk.contains(i)) => {
                let merge_key = if input == StreamKind::Upsert {
                    stream_key
                } else {
                    pk
                };
                let writer = if preserve_row_level_changes {
                    // The target table applies its own conflict semantics to every change.
                    if pk.iter().all(|i| stream_key.contains(i)) {
                        WriterCompaction::None
                    } else {
                        WriterCompaction::SplitUpdates { key: pk.to_vec() }
                    }
                } else if force_compaction {
                    WriterCompaction::PerEpoch(EpochCompaction::Once(compaction_by(
                        merge_key, input,
                    )))
                } else if skip_writer_compaction && stream_key == pk {
                    // Skipping is only valid when the keys are exactly the same, not just matching
                    // by being a subset.
                    WriterCompaction::None
                } else {
                    WriterCompaction::PerChunk(compaction_by(merge_key, input))
                };
                // The reader merges by the downstream pk, not `merge_key`: of the two, only the
                // downstream pk is sure to contain the distribution key.
                let reader = match &writer {
                    WriterCompaction::PerEpoch(_) => None,
                    _ => has_log_store.then(|| ReaderCompaction {
                        key: pk.to_vec(),
                        output,
                    }),
                };
                Self {
                    drop_deletes_first: ignore_delete,
                    writer,
                    reader,
                }
            }
            _ => {
                // With a downstream pk, keep the old row of each update, so that `then` folds a
                // retract stream and reports a downstream pk that is not unique. Upsert input has
                // no old rows to keep.
                let keep_old_rows = downstream_pk.is_some() && input != StreamKind::Upsert;
                // A log store in front of the sink keeps the changes of a stream key in order only
                // within a downstream pk, so retract changes are merged within one. An update that
                // moves a row to another downstream pk is then a delete and an insert, as it has to
                // be for the old pk to be deleted. Upsert changes can only be merged by the stream
                // key.
                let key = match &downstream_pk {
                    Some(pk) if input != StreamKind::Upsert => union(stream_key, pk),
                    _ => stream_key.to_vec(),
                };
                let by_stream_key = Compaction {
                    key,
                    input,
                    output: if keep_old_rows {
                        output_kind::RETRACT
                    } else {
                        output
                    },
                    ib: InconsistencyBehavior::Panic,
                };
                let then = match downstream_pk.as_deref() {
                    Some(pk) if !preserve_row_level_changes => {
                        EpochFinish::ByDownstreamPk(compaction_by(pk, by_stream_key.emits()))
                    }
                    Some(pk) if output == output_kind::UPSERT => {
                        EpochFinish::DropReinsertedDeletes {
                            downstream_pk: pk.to_vec(),
                        }
                    }
                    _ => EpochFinish::None,
                };
                Self {
                    drop_deletes_first: ignore_delete,
                    writer: WriterCompaction::PerEpoch(EpochCompaction::Reorder {
                        by_stream_key,
                        then,
                    }),
                    reader: None,
                }
            }
        }
    }
}
