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

//! Tests of the rules by which the sink executor compacts changes, as decided in
//! [`super::compaction_plan`].

use risingwave_common::catalog::{ColumnDesc, ColumnId};
use risingwave_common::util::epoch::test_epoch;
use risingwave_connector::sink::build_sink;
use risingwave_connector::sink::catalog::SinkType;
use risingwave_pb::stream_plan::stream_node::StreamKind;

use super::*;
use crate::common::log_store_impl::in_mem::BoundedInMemLogStoreFactory;
use crate::executor::test_utils::*;

/// Runs a sink executor over `Int64` columns and returns the output chunks.
async fn run_sink_executor(
    properties: BTreeMap<String, String>,
    sink_type: SinkType,
    input_kind: StreamKind,
    stream_key: Vec<usize>,
    downstream_pk: Vec<usize>,
    input: Vec<Message>,
) -> Vec<StreamChunk> {
    let num_columns = input
        .iter()
        .find_map(|msg| msg.as_chunk())
        .unwrap()
        .columns()
        .len();
    let columns = (0..num_columns)
        .map(|i| ColumnCatalog {
            column_desc: ColumnDesc::unnamed(ColumnId::new(i as i32), DataType::Int64),
            is_hidden: false,
        })
        .collect_vec();
    let schema: Schema = columns
        .iter()
        .map(|column| Field::from(column.column_desc.clone()))
        .collect();
    let num_barriers = input.iter().filter(|msg| msg.is_barrier()).count();

    let source = Executor::new(
        ExecutorInfo {
            stream_kind: input_kind,
            ..ExecutorInfo::for_test(
                schema.clone(),
                stream_key.clone(),
                "MockSource".to_owned(),
                0,
            )
        },
        MockSource::with_messages(input).boxed(),
    );
    let ignore_delete = properties
        .get(risingwave_connector::sink::SINK_USER_IGNORE_DELETE_OPTION)
        .is_some_and(|v| v.eq_ignore_ascii_case("true"));
    let sink_param = SinkParam {
        sink_id: 0.into(),
        sink_name: "test".into(),
        properties,
        columns: columns.iter().map(|col| col.column_desc.clone()).collect(),
        downstream_pk: Some(downstream_pk),
        sink_type,
        ignore_delete,
        format_desc: None,
        db_name: "test".into(),
        sink_from_name: "test".into(),
    };
    let info = ExecutorInfo::for_test(schema, stream_key, "SinkExecutor".to_owned(), 0);
    let sink = build_sink(sink_param.clone()).unwrap();
    let mut executor = SinkExecutor::new(
        ActorContext::for_test(0),
        info,
        source,
        SinkWriterParam::for_test(),
        sink,
        sink_param,
        columns,
        BoundedInMemLogStoreFactory::for_test(1),
        1024,
        vec![DataType::Int64; num_columns],
        None,
    )
    .unwrap()
    .boxed()
    .execute();

    let mut output = vec![];
    for _ in 0..num_barriers {
        loop {
            match executor.next().await.unwrap().unwrap() {
                Message::Chunk(chunk) => output.push(chunk.compact_vis()),
                Message::Barrier(_) => break,
                Message::Watermark(_) => unreachable!(),
            }
        }
    }
    output
}

// When the downstream pk contains the stream key, each chunk is compacted by the downstream pk.
/// An upsert sink drops the old row of an update, which a retract sink keeps.
#[tokio::test]
async fn test_retract_input_compacted_per_chunk() {
    use risingwave_common::array::StreamChunkTestExt;

    // Row 1 is inserted and updated, row 2 deleted and inserted again, and row 3 inserted and
    // deleted, all in one chunk.
    let input = || {
        vec![
            Message::Barrier(Barrier::new_test_barrier(test_epoch(1))),
            Message::Chunk(StreamChunk::from_pretty(
                "  I I
                 + 1 10
                U- 1 10
                U+ 1 20
                 - 2 5
                 + 2 6
                 + 3 30
                 - 3 30",
            )),
            Message::Barrier(Barrier::new_test_barrier(test_epoch(2))),
        ]
    };
    for (sink_type, expected) in [
        (
            SinkType::Upsert,
            " I I
            + 1 20
            + 2 6",
        ),
        (
            SinkType::Retract,
            "  I I
             + 1 20
            U- 2 5
            U+ 2 6",
        ),
    ] {
        let output = run_sink_executor(
            maplit::btreemap! { "connector".into() => "table".into() },
            sink_type,
            StreamKind::Retract,
            vec![0],
            vec![0],
            input(),
        )
        .await;
        assert_eq!(
            StreamChunk::concat(output),
            StreamChunk::from_pretty(expected),
            "{sink_type:?}"
        );
    }
}

#[tokio::test]
async fn test_upsert_input_retains_delete() {
    use risingwave_common::array::StreamChunkTestExt;

    for force_compaction in [false, true] {
        for separate_chunks in [false, true] {
            let mut input = vec![
                Message::Barrier(Barrier::new_test_barrier(test_epoch(1))),
                Message::Chunk(StreamChunk::from_pretty("I I\n+ 1 10")),
                Message::Barrier(Barrier::new_test_barrier(test_epoch(2))),
            ];
            if separate_chunks {
                input.push(Message::Chunk(StreamChunk::from_pretty("I I\n+ 1 20")));
                input.push(Message::Chunk(StreamChunk::from_pretty("I I\n- 1 .")));
            } else {
                input.push(Message::Chunk(StreamChunk::from_pretty(
                    "I I\n+ 1 20\n- 1 .",
                )));
            }
            input.push(Message::Barrier(Barrier::new_test_barrier(test_epoch(3))));
            let ctx = ActorContext::for_test(0);
            let metrics =
                ctx.streaming_metrics
                    .new_sink_exec_metrics(0.into(), ctx.id, ctx.fragment_id);
            let plan = CompactionPlan::new(&CompactionConfig {
                sink_type: SinkType::Upsert,
                input_kind: StreamKind::Upsert,
                stream_key: &[0],
                downstream_pk: Some(&[0]),
                force_compaction,
                preserve_row_level_changes: false,
                skip_writer_compaction: false,
                ignore_delete: false,
                has_log_store: true,
            });
            let output = SinkExecutor::<BoundedInMemLogStoreFactory>::process_msg(
                futures::stream::iter(input.into_iter().map(Ok)),
                plan.ignore_delete,
                plan.writer,
                1024,
                vec![DataType::Int64; 2],
                metrics.sink_chunk_buffer_size,
            )
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
            let chunks = output
                .into_iter()
                .filter_map(|msg| match msg {
                    Message::Chunk(chunk) if chunk.has_visible_rows() => Some(chunk),
                    _ => None,
                })
                .collect_vec();
            let expected = if separate_chunks && !force_compaction {
                StreamChunk::from_pretty("I I\n+ 1 10\n+ 1 20\n- 1 .")
            } else {
                StreamChunk::from_pretty("I I\n+ 1 10\n- 1 .")
            };
            assert_eq!(StreamChunk::concat(chunks), expected);
        }
    }
}

/// The blackhole sink skips the compaction for benchmarks only when the stream key is the
/// downstream pk, and otherwise compacts each chunk as other sinks do.
#[tokio::test]
async fn test_blackhole_sink_skips_compaction_only_for_same_keys() {
    use risingwave_common::array::StreamChunkTestExt;

    let input = || {
        vec![
            Message::Barrier(Barrier::new_test_barrier(test_epoch(1))),
            Message::Chunk(StreamChunk::from_pretty(
                "  I I I
                 + 1 1 10
                U- 1 1 10
                U+ 1 1 20",
            )),
            Message::Barrier(Barrier::new_test_barrier(test_epoch(2))),
        ]
    };
    for (downstream_pk, expected) in [
        (
            vec![0],
            "  I I I
             + 1 1 10
            U- 1 1 10
            U+ 1 1 20",
        ),
        // The downstream pk `0, 1` strictly contains the stream key `0`.
        (
            vec![0, 1],
            " I I I
            + 1 1 20",
        ),
    ] {
        let output = run_sink_executor(
            maplit::btreemap! { "connector".into() => "blackhole".into() },
            SinkType::Upsert,
            StreamKind::Retract,
            vec![0],
            downstream_pk.clone(),
            input(),
        )
        .await;
        assert_eq!(
            StreamChunk::concat(output),
            StreamChunk::from_pretty(expected),
            "{downstream_pk:?}"
        );
    }
}

// The reader compacts each chunk read from the log store again.
/// The log store may read the chunks written to it back as one chunk. An upsert sink keeps a delete
/// that follows an insert from an earlier chunk, as the insert may have overwritten a row the
/// downstream has, while a retract sink drops both.
#[test]
fn test_reader_compacts_chunks_merged_by_log_store() {
    use risingwave_common::array::StreamChunkTestExt;

    for (sink_type, input_kind, written, expected) in [
        // An update, then a delete in the next chunk.
        (
            SinkType::Upsert,
            StreamKind::Retract,
            [
                "  I I
                U- 1 5
                U+ 1 10",
                " I I
                - 1 10",
            ],
            Some(
                " I I
                - 1 10",
            ),
        ),
        (
            SinkType::Upsert,
            StreamKind::Upsert,
            [
                " I I
                + 1 10",
                " I I
                - 1 .",
            ],
            Some(
                " I I
                - 1 .",
            ),
        ),
        // A new row, then its delete in the next chunk.
        (
            SinkType::Retract,
            StreamKind::Retract,
            [
                " I I
                + 1 10",
                " I I
                - 1 10",
            ],
            None,
        ),
        // Two updates in two chunks.
        (
            SinkType::Retract,
            StreamKind::Retract,
            [
                "  I I
                U- 1 5
                U+ 1 10",
                "  I I
                U- 1 10
                U+ 1 20",
            ],
            Some(
                " I I
                - 1 5
                + 1 20",
            ),
        ),
    ] {
        let plan = CompactionPlan::new(&CompactionConfig {
            sink_type,
            input_kind,
            stream_key: &[0],
            downstream_pk: Some(&[0]),
            force_compaction: false,
            preserve_row_level_changes: false,
            skip_writer_compaction: false,
            ignore_delete: false,
            has_log_store: true,
        });
        let WriterCompaction::PerChunk(writer) = &plan.writer else {
            panic!("{sink_type:?}: {:?}", plan.writer);
        };
        let written = written
            .map(|chunk| (writer.compact_chunk(StreamChunk::from_pretty(chunk))).compact_vis());
        let reader = plan.reader.as_ref().unwrap();
        let read = (reader.compact_chunk(StreamChunk::concat(written.to_vec()))).compact_vis();
        match expected {
            Some(expected) => assert_eq!(
                read,
                StreamChunk::from_pretty(expected),
                "{sink_type:?} {input_kind:?}"
            ),
            None => assert_eq!(read.cardinality(), 0, "{sink_type:?} {input_kind:?}"),
        }
    }
}

// Otherwise, or with `force_compaction`, each epoch is compacted: by the stream key with deletes
// moved first, then by the downstream pk.
#[tokio::test]
async fn stream_key_sink_pk_mismatch_upsert() {
    stream_key_sink_pk_mismatch(SinkType::Upsert).await;
}

#[tokio::test]
async fn stream_key_sink_pk_mismatch_retract() {
    stream_key_sink_pk_mismatch(SinkType::Retract).await;
}

async fn stream_key_sink_pk_mismatch(sink_type: SinkType) {
    use risingwave_common::array::StreamChunkTestExt;
    use risingwave_common::array::stream_chunk::StreamChunk;
    use risingwave_common::types::DataType;

    use crate::executor::Barrier;

    let properties = maplit::btreemap! {
        "connector".into() => "blackhole".into(),
    };

    // We have two visible columns and one hidden column. The hidden column will be pruned out
    // within the sink executor.
    let columns = vec![
        ColumnCatalog {
            column_desc: ColumnDesc::unnamed(ColumnId::new(0), DataType::Int64),
            is_hidden: false,
        },
        ColumnCatalog {
            column_desc: ColumnDesc::unnamed(ColumnId::new(1), DataType::Int64),
            is_hidden: false,
        },
        ColumnCatalog {
            column_desc: ColumnDesc::unnamed(ColumnId::new(2), DataType::Int64),
            is_hidden: true,
        },
    ];
    let schema: Schema = columns
        .iter()
        .map(|column| Field::from(column.column_desc.clone()))
        .collect();

    let source = MockSource::with_messages(vec![
        Message::Barrier(Barrier::new_test_barrier(test_epoch(1))),
        Message::Chunk(std::mem::take(&mut StreamChunk::from_pretty(
            " I I I
                + 1 1 10",
        ))),
        Message::Barrier(Barrier::new_test_barrier(test_epoch(2))),
        Message::Chunk(std::mem::take(&mut StreamChunk::from_pretty(
            " I I I
                + 1 3 30",
        ))),
        Message::Chunk(std::mem::take(&mut StreamChunk::from_pretty(
            " I I I
                + 1 2 20
                - 1 2 20",
        ))),
        Message::Chunk(std::mem::take(&mut StreamChunk::from_pretty(
            " I I I
                - 1 1 10
                + 1 1 40",
        ))),
        Message::Barrier(Barrier::new_test_barrier(test_epoch(3))),
    ])
    .into_executor(schema.clone(), vec![0, 1]);

    let sink_param = SinkParam {
        sink_id: 0.into(),
        sink_name: "test".into(),
        properties,

        columns: columns
            .iter()
            .filter(|col| !col.is_hidden)
            .map(|col| col.column_desc.clone())
            .collect(),
        downstream_pk: Some(vec![0]),
        sink_type,
        ignore_delete: false,
        format_desc: None,
        db_name: "test".into(),
        sink_from_name: "test".into(),
    };

    let info = ExecutorInfo::for_test(schema, vec![0, 1], "SinkExecutor".to_owned(), 0);

    let sink = build_sink(sink_param.clone()).unwrap();

    let sink_executor = SinkExecutor::new(
        ActorContext::for_test(0),
        info,
        source,
        SinkWriterParam::for_test(),
        sink,
        sink_param,
        columns.clone(),
        BoundedInMemLogStoreFactory::for_test(1),
        1024,
        vec![DataType::Int64, DataType::Int64, DataType::Int64],
        None,
    )
    .unwrap();

    let mut executor = sink_executor.boxed().execute();

    // Barrier message.
    executor.next().await.unwrap().unwrap();

    let chunk_msg = executor.next().await.unwrap().unwrap();
    assert_eq!(
        chunk_msg.into_chunk().unwrap().compact_vis(),
        StreamChunk::from_pretty(
            " I I I
            + 1 1 10",
        )
    );

    // Barrier message.
    executor.next().await.unwrap().unwrap();

    let chunk_msg = executor.next().await.unwrap().unwrap();
    let expected = match sink_type {
        SinkType::Retract => StreamChunk::from_pretty(
            " I I I
            U- 1 1 10
            U+ 1 1 40",
        ),
        SinkType::Upsert => StreamChunk::from_pretty(
            " I I I
            + 1 1 40", // For upsert format, there won't be `U- 1 1 10`.
        ),
        _ => unreachable!(),
    };
    assert_eq!(chunk_msg.into_chunk().unwrap().compact_vis(), expected);

    // The last barrier message.
    executor.next().await.unwrap().unwrap();
}

/// The stream key `(0, 1)` does not cover the sink pk `(0, 2)`, so an update of a stream key
/// may change its sink pk.
fn sink_pk_change_input() -> Vec<Message> {
    use risingwave_common::array::StreamChunkTestExt;

    vec![
        Message::Barrier(Barrier::new_test_barrier(test_epoch(1))),
        Message::Chunk(StreamChunk::from_pretty(
            " I I I I
            + 1 1 6 10
            + 2 2 6 10",
        )),
        Message::Barrier(Barrier::new_test_barrier(test_epoch(2))),
        // The sink pk of `(1, 1)` changes, while that of `(2, 2)` does not.
        Message::Chunk(StreamChunk::from_pretty(
            "  I I I I
            U- 1 1 6 10
            U+ 1 1 8 10
            U- 2 2 6 10
            U+ 2 2 6 20",
        )),
        Message::Barrier(Barrier::new_test_barrier(test_epoch(3))),
    ]
}

#[tokio::test]
async fn test_sink_pk_change_upsert() {
    test_sink_pk_change(SinkType::Upsert).await;
}

#[tokio::test]
async fn test_sink_pk_change_retract() {
    test_sink_pk_change(SinkType::Retract).await;
}

async fn test_sink_pk_change(sink_type: SinkType) {
    use risingwave_common::array::StreamChunkTestExt;

    let output = run_sink_executor(
        maplit::btreemap! { "connector".into() => "blackhole".into() },
        sink_type,
        StreamKind::Retract,
        vec![0, 1],
        vec![0, 2],
        sink_pk_change_input(),
    )
    .await;

    let expected = match sink_type {
        SinkType::Upsert => StreamChunk::from_pretty(
            " I I I I
            - 1 1 6 10
            + 2 2 6 20
            + 1 1 8 10",
        ),
        SinkType::Retract => StreamChunk::from_pretty(
            "  I I I I
            -  1 1 6 10
            U- 2 2 6 10
            U+ 2 2 6 20
            +  1 1 8 10",
        ),
        _ => unreachable!(),
    };
    assert_eq!(
        output,
        vec![
            StreamChunk::from_pretty(
                " I I I I
                + 1 1 6 10
                + 2 2 6 10",
            ),
            expected,
        ]
    );
}

/// The old row of an upsert input only guarantees the stream key columns, so its sink pk is
/// unknown and no delete is emitted for it.
#[tokio::test]
async fn test_upsert_input_sink_pk_change() {
    use risingwave_common::array::StreamChunkTestExt;

    let input = vec![
        Message::Barrier(Barrier::new_test_barrier(test_epoch(1))),
        Message::Chunk(StreamChunk::from_pretty(
            " I I I I
            + 1 1 6 10",
        )),
        Message::Barrier(Barrier::new_test_barrier(test_epoch(2))),
        Message::Chunk(StreamChunk::from_pretty(
            " I I I I
            - 1 1 . .
            + 1 1 8 10",
        )),
        Message::Barrier(Barrier::new_test_barrier(test_epoch(3))),
    ];
    let output = run_sink_executor(
        maplit::btreemap! { "connector".into() => "blackhole".into() },
        SinkType::Upsert,
        StreamKind::Upsert,
        vec![0, 1],
        vec![0, 2],
        input,
    )
    .await;

    assert_eq!(
        output,
        vec![
            StreamChunk::from_pretty(
                " I I I I
                + 1 1 6 10",
            ),
            StreamChunk::from_pretty(
                " I I I I
                + 1 1 8 10",
            ),
        ]
    );
}

#[tokio::test]
async fn test_force_compaction() {
    use risingwave_common::array::StreamChunkTestExt;
    use risingwave_common::array::stream_chunk::StreamChunk;
    use risingwave_common::types::DataType;

    use crate::executor::Barrier;

    let properties = maplit::btreemap! {
        "connector".into() => "blackhole".into(),
        "force_compaction".into() => "true".into()
    };

    // We have two visible columns and one hidden column. The hidden column will be pruned out
    // within the sink executor.
    let columns = vec![
        ColumnCatalog {
            column_desc: ColumnDesc::unnamed(ColumnId::new(0), DataType::Int64),
            is_hidden: false,
        },
        ColumnCatalog {
            column_desc: ColumnDesc::unnamed(ColumnId::new(1), DataType::Int64),
            is_hidden: false,
        },
        ColumnCatalog {
            column_desc: ColumnDesc::unnamed(ColumnId::new(2), DataType::Int64),
            is_hidden: true,
        },
    ];
    let schema: Schema = columns
        .iter()
        .map(|column| Field::from(column.column_desc.clone()))
        .collect();

    let source = MockSource::with_messages(vec![
        Message::Barrier(Barrier::new_test_barrier(test_epoch(1))),
        Message::Chunk(StreamChunk::from_pretty(
            " I I I
                + 1 1 10",
        )),
        Message::Barrier(Barrier::new_test_barrier(test_epoch(2))),
        Message::Chunk(StreamChunk::from_pretty(
            " I I I
                + 1 3 30",
        )),
        Message::Chunk(StreamChunk::from_pretty(
            " I I I
                + 1 2 20
                - 1 2 20
                + 1 4 10",
        )),
        Message::Chunk(StreamChunk::from_pretty(
            " I I I
                - 1 1 10
                + 1 1 40",
        )),
        Message::Chunk(StreamChunk::from_pretty(
            " I I I
                - 1 4 30",
        )),
        Message::Barrier(Barrier::new_test_barrier(test_epoch(3))),
    ])
    .into_executor(schema.clone(), vec![0, 1]);

    let sink_param = SinkParam {
        sink_id: 0.into(),
        sink_name: "test".into(),
        properties,

        columns: columns
            .iter()
            .filter(|col| !col.is_hidden)
            .map(|col| col.column_desc.clone())
            .collect(),
        downstream_pk: Some(vec![0, 1]),
        sink_type: SinkType::Upsert,
        ignore_delete: false,
        format_desc: None,
        db_name: "test".into(),
        sink_from_name: "test".into(),
    };

    let info = ExecutorInfo::for_test(schema, vec![0, 1], "SinkExecutor".to_owned(), 0);

    let sink = build_sink(sink_param.clone()).unwrap();

    let sink_executor = SinkExecutor::new(
        ActorContext::for_test(0),
        info,
        source,
        SinkWriterParam::for_test(),
        sink,
        sink_param,
        columns.clone(),
        BoundedInMemLogStoreFactory::for_test(1),
        1024,
        vec![DataType::Int64, DataType::Int64, DataType::Int64],
        None,
    )
    .unwrap();

    let mut executor = sink_executor.boxed().execute();

    // Barrier message.
    executor.next().await.unwrap().unwrap();

    let chunk_msg = executor.next().await.unwrap().unwrap();
    assert_eq!(
        chunk_msg.into_chunk().unwrap().compact_vis(),
        StreamChunk::from_pretty(
            " I I I
            + 1 1 10",
        )
    );

    // Barrier message.
    executor.next().await.unwrap().unwrap();

    let chunk_msg = executor.next().await.unwrap().unwrap();
    assert_eq!(
        chunk_msg.into_chunk().unwrap().compact_vis(),
        StreamChunk::from_pretty(
            " I I I
            + 1 1 40
            + 1 3 30",
        )
    );

    // The last barrier message.
    executor.next().await.unwrap().unwrap();
}

/// With `force_compaction`, records are reordered even though the stream key `(0)` is covered by
/// the sink pk `(0, 1)`, where an update may change the sink pk as well.
#[tokio::test]
async fn test_force_compaction_sink_pk_change() {
    use risingwave_common::array::StreamChunkTestExt;

    let input = vec![
        Message::Barrier(Barrier::new_test_barrier(test_epoch(1))),
        Message::Chunk(StreamChunk::from_pretty(
            " I I I
            + 1 6 10",
        )),
        Message::Barrier(Barrier::new_test_barrier(test_epoch(2))),
        Message::Chunk(StreamChunk::from_pretty(
            "  I I I
            U- 1 6 10
            U+ 1 8 10",
        )),
        Message::Barrier(Barrier::new_test_barrier(test_epoch(3))),
    ];
    let output = run_sink_executor(
        maplit::btreemap! {
            "connector".into() => "blackhole".into(),
            "force_compaction".into() => "true".into()
        },
        SinkType::Upsert,
        StreamKind::Retract,
        vec![0],
        vec![0, 1],
        input,
    )
    .await;

    assert_eq!(
        output,
        vec![
            StreamChunk::from_pretty(
                " I I I
                + 1 6 10",
            ),
            StreamChunk::from_pretty(
                " I I I
                - 1 6 10
                + 1 8 10",
            ),
        ]
    );
}

#[tokio::test]
async fn test_sink_into_table_keeps_default_compaction_without_special_conflict_semantics() {
    use risingwave_common::array::StreamChunkTestExt;
    use risingwave_common::array::stream_chunk::StreamChunk;
    use risingwave_common::types::DataType;

    use crate::executor::Barrier;

    let properties = maplit::btreemap! {
        "connector".into() => "table".into(),
    };

    let columns = vec![
        ColumnCatalog {
            column_desc: ColumnDesc::unnamed(ColumnId::new(0), DataType::Int64),
            is_hidden: false,
        },
        ColumnCatalog {
            column_desc: ColumnDesc::unnamed(ColumnId::new(1), DataType::Int64),
            is_hidden: false,
        },
    ];
    let schema: Schema = columns
        .iter()
        .map(|column| Field::from(column.column_desc.clone()))
        .collect();

    let source = MockSource::with_messages(vec![
        Message::Barrier(Barrier::new_test_barrier(test_epoch(1))),
        Message::Chunk(StreamChunk::from_pretty(
            " I  I
              + 1 10
              + 1 20",
        )),
        Message::Barrier(Barrier::new_test_barrier(test_epoch(2))),
    ])
    .into_executor(schema.clone(), vec![0]);

    let sink_param = SinkParam {
        sink_id: 0.into(),
        sink_name: "test".into(),
        properties,
        columns: columns.iter().map(|col| col.column_desc.clone()).collect(),
        downstream_pk: Some(vec![0]),
        sink_type: SinkType::Upsert,
        ignore_delete: false,
        format_desc: None,
        db_name: "test".into(),
        sink_from_name: "test".into(),
    };

    let info = ExecutorInfo::for_test(schema, vec![0], "SinkExecutor".to_owned(), 0);
    let sink = build_sink(sink_param.clone()).unwrap();

    let sink_executor = SinkExecutor::new(
        ActorContext::for_test(0),
        info,
        source,
        SinkWriterParam::for_test(),
        sink,
        sink_param,
        columns,
        BoundedInMemLogStoreFactory::for_test(1),
        1024,
        vec![DataType::Int64, DataType::Int64],
        None,
    )
    .unwrap();

    let mut executor = sink_executor.boxed().execute();

    executor.next().await.unwrap().unwrap();

    let chunk_msg = executor.next().await.unwrap().unwrap();
    assert_eq!(
        chunk_msg.into_chunk().unwrap().compact_vis(),
        StreamChunk::from_pretty(
            " I  I
              + 1 20",
        )
    );

    executor.next().await.unwrap().unwrap();
}

// With `preserve_row_level_changes`, nothing is compacted by the downstream pk.
#[tokio::test]
async fn test_sink_into_table_preserves_special_conflict_rows_for_mismatched_pk() {
    use risingwave_common::array::StreamChunkTestExt;
    use risingwave_common::array::stream_chunk::StreamChunk;
    use risingwave_common::types::DataType;

    use crate::executor::Barrier;

    let properties = maplit::btreemap! {
        "connector".into() => "table".into(),
        SINK_USER_PRESERVE_ROW_LEVEL_CHANGES.into() => "true".into(),
    };

    let columns = vec![
        ColumnCatalog {
            column_desc: ColumnDesc::unnamed(ColumnId::new(0), DataType::Int64),
            is_hidden: false,
        },
        ColumnCatalog {
            column_desc: ColumnDesc::unnamed(ColumnId::new(1), DataType::Int64),
            is_hidden: false,
        },
        ColumnCatalog {
            column_desc: ColumnDesc::unnamed(ColumnId::new(2), DataType::Int64),
            is_hidden: false,
        },
    ];
    let schema: Schema = columns
        .iter()
        .map(|column| Field::from(column.column_desc.clone()))
        .collect();

    let source = MockSource::with_messages(vec![
        Message::Barrier(Barrier::new_test_barrier(test_epoch(1))),
        Message::Chunk(StreamChunk::from_pretty(
            " I  I  I
              + 1 10  1",
        )),
        Message::Chunk(StreamChunk::from_pretty(
            " I  I  I
              + 1 20  2",
        )),
        Message::Barrier(Barrier::new_test_barrier(test_epoch(2))),
        // Downstream key 1 moves to a new stream key, while downstream key 2 is really deleted.
        Message::Chunk(StreamChunk::from_pretty(
            " I  I  I
              - 1 10  1
              - 1 20  2
              + 1 30  3
              - 2 50  5",
        )),
        Message::Barrier(Barrier::new_test_barrier(test_epoch(3))),
    ])
    .into_executor(schema.clone(), vec![0, 2]);

    let sink_param = SinkParam {
        sink_id: 0.into(),
        sink_name: "test".into(),
        properties,
        columns: columns.iter().map(|col| col.column_desc.clone()).collect(),
        downstream_pk: Some(vec![0]),
        sink_type: SinkType::Upsert,
        ignore_delete: false,
        format_desc: None,
        db_name: "test".into(),
        sink_from_name: "test".into(),
    };

    let info = ExecutorInfo::for_test(schema, vec![0, 2], "SinkExecutor".to_owned(), 0);
    let sink = build_sink(sink_param.clone()).unwrap();

    let sink_executor = SinkExecutor::new(
        ActorContext::for_test(0),
        info,
        source,
        SinkWriterParam::for_test(),
        sink,
        sink_param,
        columns,
        BoundedInMemLogStoreFactory::for_test(1),
        1024,
        vec![DataType::Int64, DataType::Int64, DataType::Int64],
        None,
    )
    .unwrap();

    let mut executor = sink_executor.boxed().execute();

    executor.next().await.unwrap().unwrap();

    let chunk_msg = executor.next().await.unwrap().unwrap();
    assert_eq!(
        chunk_msg.into_chunk().unwrap().compact_vis(),
        StreamChunk::from_pretty(
            " I  I  I
              + 1 10  1
              + 1 20  2",
        )
    );

    executor.next().await.unwrap().unwrap();

    let chunk_msg = executor.next().await.unwrap().unwrap();
    assert_eq!(
        chunk_msg.into_chunk().unwrap().compact_vis(),
        StreamChunk::from_pretty(
            " I  I  I
              - 2 50  5
              + 1 30  3",
        )
    );

    executor.next().await.unwrap().unwrap();
}

#[tokio::test]
async fn test_sink_into_table_preserves_special_conflict_rows_for_pk_change() {
    use risingwave_common::array::StreamChunkTestExt;

    let output = run_sink_executor(
        maplit::btreemap! {
            "connector".into() => "table".into(),
            SINK_USER_PRESERVE_ROW_LEVEL_CHANGES.into() => "true".into()
        },
        SinkType::Upsert,
        StreamKind::Retract,
        vec![0, 1],
        vec![0, 2],
        sink_pk_change_input(),
    )
    .await;

    // The delete of the unchanged sink pk `(2, 6)` is dropped, as the key is inserted again.
    assert_eq!(
        output,
        vec![
            StreamChunk::from_pretty(
                " I I I I
                + 1 1 6 10
                + 2 2 6 10",
            ),
            StreamChunk::from_pretty(
                " I I I I
                - 1 1 6 10
                + 1 1 8 10
                + 2 2 6 20",
            ),
        ]
    );
}

// With `ignore_delete`, the deletes are dropped first, and the rest is compacted by the downstream
// pk.
/// With `ignore_delete`, every inserted or updated row reaches the downstream, however the sink
/// compacts the changes.
#[tokio::test]
async fn test_ignore_delete_keeps_every_inserted_row() {
    use risingwave_common::array::StreamChunkTestExt;

    type Inputs = Vec<(StreamKind, Vec<Message>)>;

    // The chunks of each epoch.
    fn epochs(epochs: &[&[&str]]) -> Vec<Message> {
        let mut messages = vec![Message::Barrier(Barrier::new_test_barrier(test_epoch(1)))];
        for (i, chunks) in epochs.iter().enumerate() {
            messages.extend(
                (chunks.iter()).map(|chunk| Message::Chunk(StreamChunk::from_pretty(chunk))),
            );
            let epoch = test_epoch(i as u64 + 2);
            messages.push(Message::Barrier(Barrier::new_test_barrier(epoch)));
        }
        messages
    }

    fn properties(extra: &[(&str, &str)]) -> BTreeMap<String, String> {
        let mut properties = maplit::btreemap! {
            "connector".to_owned() => "table".to_owned(),
            "ignore_delete".to_owned() => "true".to_owned(),
        };
        properties.extend(extra.iter().map(|(k, v)| (k.to_string(), v.to_string())));
        properties
    }

    // A delete arrives with the insert or update before it, in the same chunk or a later one.
    fn deletes_after_changes() -> Inputs {
        vec![
            (
                StreamKind::Retract,
                epochs(&[
                    &[" I I
                        + 3 30"],
                    &[
                        "  I I
                         + 1 10
                         - 1 10
                        U- 3 30
                        U+ 3 31
                         - 3 31
                         + 2 20",
                        " I I
                        - 2 20",
                    ],
                ]),
            ),
            (
                StreamKind::Upsert,
                epochs(&[
                    &[" I I
                        + 3 30"],
                    &[
                        " I I
                        + 1 10
                        - 1 .
                        + 3 31
                        - 3 .
                        + 2 20",
                        " I I
                        - 2 .",
                    ],
                ]),
            ),
        ]
    }

    // An update moves stream key `1` from downstream pk `6` to `8`.
    fn row_moved() -> Inputs {
        vec![
            (
                StreamKind::Retract,
                epochs(&[&["  I I
                             + 1 6
                            U- 1 6
                            U+ 1 8"]]),
            ),
            (
                StreamKind::Upsert,
                epochs(&[&[" I I
                            + 1 6
                            + 1 8"]]),
            ),
        ]
    }

    // Two stream keys insert downstream pk `1` in one epoch. There are only inserts, so the
    // input kind does not matter.
    fn same_pk_inserted_twice() -> Inputs {
        vec![(
            StreamKind::Retract,
            epochs(&[&[" I I I
                        + 1 1 10
                        + 1 2 20"]]),
        )]
    }

    // Each scenario runs with each sink configuration listed with it.
    let scenarios = [
        (
            deletes_after_changes as fn() -> Inputs,
            " I I
            + 1 10
            + 2 20
            + 3 30
            + 3 31",
            vec![
                // Compacts each chunk.
                (properties(&[]), vec![0], vec![0]),
                // Buffers and compacts each epoch.
                (
                    properties(&[
                        ("connector", "blackhole"),
                        (SINK_USER_FORCE_COMPACTION, "true"),
                    ]),
                    vec![0],
                    vec![0],
                ),
            ],
        ),
        (
            row_moved,
            " I I
            + 1 6
            + 1 8",
            vec![
                // The stream key `0` does not determine the downstream pk `1`.
                (properties(&[]), vec![0], vec![1]),
                // The downstream pk `0, 1` contains the stream key, but each epoch is compacted.
                (
                    properties(&[(SINK_USER_FORCE_COMPACTION, "true")]),
                    vec![0],
                    vec![0, 1],
                ),
            ],
        ),
        (
            same_pk_inserted_twice,
            " I I I
            + 1 1 10
            + 1 2 20",
            // The target table's conflict handling must see each insert, with or without
            // `force_compaction`.
            ["false", "true"]
                .map(|force_compaction| {
                    let properties = properties(&[
                        (SINK_USER_FORCE_COMPACTION, force_compaction),
                        (SINK_USER_PRESERVE_ROW_LEVEL_CHANGES, "true"),
                    ]);
                    (properties, vec![0, 1], vec![0])
                })
                .to_vec(),
        ),
    ];

    for (inputs, expected, configurations) in scenarios {
        let expected = StreamChunk::from_pretty(expected).sort_rows();
        for (properties, stream_key, downstream_pk) in configurations {
            for (input_kind, input) in inputs() {
                let output = run_sink_executor(
                    properties.clone(),
                    SinkType::Upsert,
                    input_kind,
                    stream_key.clone(),
                    downstream_pk.clone(),
                    input,
                )
                .await;
                assert_eq!(
                    StreamChunk::concat(output).sort_rows(),
                    expected,
                    "{properties:?} {input_kind:?} downstream pk {downstream_pk:?}"
                );
            }
        }
    }
}

/// With `ignore_delete`, the inserts left after dropping the deletes are compacted by the
/// downstream pk: an insert that a later insert of its downstream pk overwrites in the same
/// chunk, or epoch with `force_compaction`, is not emitted.
#[tokio::test]
async fn test_ignore_delete_compacts_by_downstream_pk() {
    use risingwave_common::array::StreamChunkTestExt;

    let input = |chunk: &str| {
        vec![
            Message::Barrier(Barrier::new_test_barrier(test_epoch(1))),
            Message::Chunk(StreamChunk::from_pretty(chunk)),
            Message::Barrier(Barrier::new_test_barrier(test_epoch(2))),
        ]
    };
    // Row `1` is inserted and updated, which leaves two inserts of it without the delete.
    let inputs = [
        (
            StreamKind::Retract,
            "  I I
             + 1 10
            U- 1 10
            U+ 1 20",
        ),
        (
            StreamKind::Upsert,
            " I I
            + 1 10
            + 1 20",
        ),
    ];
    let expected = StreamChunk::from_pretty(
        " I I
        + 1 20",
    );

    for force_compaction in ["false", "true"] {
        let properties = maplit::btreemap! {
            "connector".into() => "table".into(),
            "ignore_delete".into() => "true".into(),
            SINK_USER_FORCE_COMPACTION.into() => force_compaction.into(),
        };
        for (input_kind, chunk) in inputs {
            let output = run_sink_executor(
                properties.clone(),
                SinkType::Upsert,
                input_kind,
                vec![0],
                vec![0],
                input(chunk),
            )
            .await;
            assert_eq!(
                StreamChunk::concat(output),
                expected,
                "{properties:?} {input_kind:?}"
            );
        }
    }
}
