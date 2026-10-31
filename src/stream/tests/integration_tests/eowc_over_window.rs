// Copyright 2023 RisingWave Labs
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

use std::collections::HashSet;
use std::ops::Bound;

use futures::{Future, StreamExt, pin_mut};
use itertools::Itertools;
use risingwave_common::bitmap::{Bitmap, BitmapBuilder};
use risingwave_common::hash::{VirtualNode, VnodeBitmapExt};
use risingwave_common::id::ActorId;
use risingwave_common::row::{self, OwnedRow, Row};
use risingwave_common::types::ScalarImpl;
use risingwave_common::util::epoch::{EpochPair, test_epoch};
use risingwave_expr::aggregate::{AggArgs, PbAggKind};
use risingwave_expr::window_function::{
    Frame, FrameBound, FrameBounds, FrameExclusion, RangeFrameBounds, RangeFrameOffset,
    SessionFrameBounds, SessionFrameGap, WindowFuncCall, WindowFuncKind,
};
use risingwave_pb::catalog::PbTable;
use risingwave_pb::stream_plan::PbEowcOverWindowNodeVersion;
use risingwave_storage::store::PrefetchOptions;
use risingwave_stream::common::table::test_utils::gen_pbtable_with_dist_key;
use risingwave_stream::executor::{
    Barrier, EowcOverWindowExecutor, EowcOverWindowExecutorArgs, Message, Mutation, UpdateMutation,
};

use crate::prelude::*;

/// Plans created before windows can be closed by watermark.
const LEGACY: PbEowcOverWindowNodeVersion = PbEowcOverWindowNodeVersion::Unspecified;
const LATEST: PbEowcOverWindowNodeVersion = PbEowcOverWindowNodeVersion::LATEST;

/// Optional state tables of the executor.
#[derive(Clone, Copy)]
enum OptionalTables {
    None,
    IntermediateState,
    /// Intermediate state table with the `progress` column, and the watermark trigger table.
    WatermarkTrigger,
}

#[derive(Clone)]
struct ExecutorOptions {
    tables: OptionalTables,
    version: PbEowcOverWindowNodeVersion,
    chunk_size: usize,
    input_stream_key: Vec<usize>,
    /// Evict all cached partitions whenever the cache is evicted.
    evict_all: bool,
    /// The vnodes owned, with the tables distributed by the partition key. The tables are
    /// singleton if `None`.
    vnodes: Option<Arc<Bitmap>>,
}

impl ExecutorOptions {
    fn new(tables: OptionalTables, version: PbEowcOverWindowNodeVersion) -> Self {
        Self {
            tables,
            version,
            chunk_size: 1024,
            input_stream_key: vec![2],
            evict_all: false,
            vnodes: None,
        }
    }
}

/// The state table has the input schema, with pk `partition key | order key | pk`. It's also the pk
/// for the input stream key `order key | pk`.
fn state_table_catalog(distributed: bool) -> PbTable {
    gen_pbtable_with_dist_key(
        TableId::new(1),
        vec![
            ColumnDesc::unnamed(ColumnId::new(0), DataType::Int64), // order key
            ColumnDesc::unnamed(ColumnId::new(1), DataType::Varchar), // partition key
            ColumnDesc::unnamed(ColumnId::new(2), DataType::Int64), // pk
            ColumnDesc::unnamed(ColumnId::new(3), DataType::Int32), // x
        ],
        vec![OrderType::ascending(); 3],
        vec![1, 0, 2],
        0,
        if distributed { vec![1] } else { vec![] },
    )
}

/// Intermediate state table: partition key (Varchar) + `state_0..state_{n-1}` (Bytea)
/// [+ progress (Bytea)]. PK = partition key columns only.
fn intermediate_state_table_catalog(
    num_calls: usize,
    with_progress: bool,
    distributed: bool,
) -> PbTable {
    let num_bytea_columns = num_calls + usize::from(with_progress);
    let columns = std::iter::once(DataType::Varchar)
        .chain(std::iter::repeat_n(DataType::Bytea, num_bytea_columns))
        .enumerate()
        .map(|(i, data_type)| ColumnDesc::unnamed(ColumnId::new(i as i32), data_type))
        .collect();
    gen_pbtable_with_dist_key(
        TableId::new(2),
        columns,
        vec![OrderType::ascending()],
        vec![0],
        1, // read_prefix_len_hint = partition key length
        if distributed { vec![0] } else { vec![] },
    )
}

/// Watermark trigger table: `trigger_watermark` (order key type) + `trigger_exclusive` (Boolean) +
/// partition key (Varchar). PK = all columns.
fn watermark_trigger_table_catalog(distributed: bool) -> PbTable {
    gen_pbtable_with_dist_key(
        TableId::new(3),
        vec![
            ColumnDesc::unnamed(ColumnId::new(0), DataType::Int64),
            ColumnDesc::unnamed(ColumnId::new(1), DataType::Boolean),
            ColumnDesc::unnamed(ColumnId::new(2), DataType::Varchar),
        ],
        vec![OrderType::ascending(); 3],
        vec![0, 1, 2],
        0,
        if distributed { vec![2] } else { vec![] },
    )
}

/// Get the rows of the table in all vnodes.
async fn table_rows(
    table: &PbTable,
    store: MemoryStateStore,
    vnodes: Option<Arc<Bitmap>>,
) -> Vec<OwnedRow> {
    let mut table = StateTable::from_table_catalog_inconsistent_op(table, store, vnodes).await;
    table
        .init_epoch(EpochPair::new_test_epoch(test_epoch(100)))
        .await
        .unwrap();
    let range: &(Bound<OwnedRow>, Bound<OwnedRow>) = &(Bound::Unbounded, Bound::Unbounded);
    let mut rows = vec![];
    for vnode in table.vnodes().iter_vnodes() {
        let iter = table
            .iter_keyed_row_with_vnode(vnode, range, PrefetchOptions::default())
            .await
            .unwrap();
        pin_mut!(iter);
        while let Some(row) = iter.next().await {
            rows.push(row.unwrap().into_owned_row());
        }
    }
    rows
}

/// Get the rows in the singleton state table.
async fn state_table_rows(store: MemoryStateStore) -> Vec<OwnedRow> {
    table_rows(&state_table_catalog(false), store, None).await
}

async fn create_executor_with_options<S: StateStore>(
    calls: Vec<WindowFuncCall>,
    store: S,
    options: ExecutorOptions,
) -> (MessageSender, BoxedMessageStream) {
    let input_schema = Schema::new(vec![
        Field::unnamed(DataType::Int64),   // order key
        Field::unnamed(DataType::Varchar), // partition key
        Field::unnamed(DataType::Int64),   // pk
        Field::unnamed(DataType::Int32),   // x
    ]);
    let partition_key_indices = vec![1];
    let order_key_index = 0;

    let output_schema = {
        let mut fields = input_schema.fields.clone();
        calls.iter().for_each(|call| {
            fields.push(Field::unnamed(call.return_type.clone()));
        });
        Schema { fields }
    };

    let distributed = options.vnodes.is_some();
    let state_table = StateTable::from_table_catalog_inconsistent_op(
        &state_table_catalog(distributed),
        store.clone(),
        options.vnodes.clone(),
    )
    .await;

    let intermediate_state_table = match options.tables {
        OptionalTables::None => None,
        OptionalTables::IntermediateState => Some(
            StateTable::from_table_catalog_inconsistent_op(
                &intermediate_state_table_catalog(calls.len(), false, distributed),
                store.clone(),
                options.vnodes.clone(),
            )
            .await,
        ),
        OptionalTables::WatermarkTrigger => Some(
            StateTable::from_table_catalog(
                &intermediate_state_table_catalog(calls.len(), true, distributed),
                store.clone(),
                options.vnodes.clone(),
            )
            .await,
        ),
    };

    let watermark_trigger_table = match options.tables {
        OptionalTables::WatermarkTrigger => Some(
            StateTable::from_table_catalog(
                &watermark_trigger_table_catalog(distributed),
                store,
                options.vnodes.clone(),
            )
            .await,
        ),
        _ => None,
    };

    let (tx, source) = MockSource::channel();
    let source = source.into_executor(input_schema, options.input_stream_key.clone());
    let executor = EowcOverWindowExecutor::new(EowcOverWindowExecutorArgs {
        actor_ctx: ActorContext::for_test(123),

        input: source,

        schema: output_schema,
        calls,
        partition_key_indices,
        order_key_index,
        state_table,
        watermark_epoch: Arc::new(AtomicU64::new(if options.evict_all { u64::MAX } else { 0 })),
        intermediate_state_table,
        watermark_trigger_table,
        version: options.version,
        chunk_size: options.chunk_size,
    });
    (tx, executor.boxed().execute())
}

async fn create_executor<S: StateStore>(
    calls: Vec<WindowFuncCall>,
    store: S,
    tables: OptionalTables,
    version: PbEowcOverWindowNodeVersion,
) -> (MessageSender, BoxedMessageStream) {
    create_executor_with_options(calls, store, ExecutorOptions::new(tables, version)).await
}

/// Create an executor whose windows may be closed by watermark.
async fn create_executor_with_watermark_trigger_table<S: StateStore>(
    calls: Vec<WindowFuncCall>,
    store: S,
    chunk_size: usize,
) -> (MessageSender, BoxedMessageStream) {
    create_executor_with_options(
        calls,
        store,
        ExecutorOptions {
            chunk_size,
            ..ExecutorOptions::new(OptionalTables::WatermarkTrigger, LATEST)
        },
    )
    .await
}

/// Check the script with the executors of both plan versions, whose outputs are the same without
/// `RANGE` or `SESSION` frames.
async fn check_with_script_of_both_versions<F, Fut>(
    create_executor: F,
    test_script: &str,
    expect: expect_test::Expect,
) where
    F: Fn(MemoryStateStore, PbEowcOverWindowNodeVersion) -> Fut,
    Fut: Future<Output = (MessageSender, BoxedMessageStream)>,
{
    let mut outputs = vec![];
    for version in [LEGACY, LATEST] {
        let store = MemoryStateStore::new();
        outputs.push(
            executor_snapshot(
                || create_executor(store.clone(), version),
                test_script,
                SnapshotOptions::default(),
            )
            .await,
        );
    }
    assert_eq!(outputs[0], outputs[1]);
    expect.assert_eq(&outputs[0]);
}

/// `count(x)` over `SESSION WITH GAP gap`.
fn session_count(gap: i64) -> WindowFuncCall {
    WindowFuncCall {
        kind: WindowFuncKind::Aggregate(PbAggKind::Count.into()),
        return_type: DataType::Int64,
        args: AggArgs::from_iter([(DataType::Int32, 3)]),
        ignore_nulls: false,
        frame: Frame {
            bounds: FrameBounds::Session(SessionFrameBounds {
                order_data_type: DataType::Int64,
                order_type: OrderType::ascending(),
                gap_data_type: DataType::Int64,
                gap: SessionFrameGap::new_for_test(
                    ScalarImpl::Int64(gap),
                    &DataType::Int64,
                    &DataType::Int64,
                ),
            }),
            exclusion: FrameExclusion::NoOthers,
        },
    }
}

/// `sum(x)` over `RANGE BETWEEN start AND end`.
fn range_sum(start: FrameBound<i64>, end: FrameBound<i64>) -> WindowFuncCall {
    let offset = |bound: FrameBound<i64>| {
        bound.map(|offset| {
            RangeFrameOffset::new_for_test(
                ScalarImpl::Int64(offset),
                &DataType::Int64,
                &DataType::Int64,
            )
        })
    };
    WindowFuncCall {
        kind: WindowFuncKind::Aggregate(PbAggKind::Sum.into()),
        return_type: DataType::Int64,
        args: AggArgs::from_iter([(DataType::Int32, 3)]),
        ignore_nulls: false,
        frame: Frame {
            bounds: FrameBounds::Range(RangeFrameBounds {
                order_data_type: DataType::Int64,
                order_type: OrderType::ascending(),
                offset_data_type: DataType::Int64,
                start: offset(start),
                end: offset(end),
            }),
            exclusion: FrameExclusion::NoOthers,
        },
    }
}

/// `lag(x, 1)` or `lead(x, 1)`.
fn lag_or_lead(lead: bool) -> WindowFuncCall {
    let bound = if lead {
        FrameBound::Following(1)
    } else {
        FrameBound::Preceding(1)
    };
    WindowFuncCall {
        kind: WindowFuncKind::Aggregate(PbAggKind::FirstValue.into()),
        return_type: DataType::Int32,
        args: AggArgs::from_iter([(DataType::Int32, 3)]),
        ignore_nulls: false,
        frame: Frame::rows(bound.clone(), bound),
    }
}

#[tokio::test]
async fn test_over_window() {
    let calls = vec![
        // lag(x, 1)
        WindowFuncCall {
            kind: WindowFuncKind::Aggregate(PbAggKind::FirstValue.into()),
            return_type: DataType::Int32,
            args: AggArgs::from_iter([(DataType::Int32, 3)]),
            ignore_nulls: false,
            frame: Frame::rows(FrameBound::Preceding(1), FrameBound::Preceding(1)),
        },
        // lead(x, 1)
        WindowFuncCall {
            kind: WindowFuncKind::Aggregate(PbAggKind::FirstValue.into()),
            return_type: DataType::Int32,
            args: AggArgs::from_iter([(DataType::Int32, 3)]),
            ignore_nulls: false,
            frame: Frame::rows(FrameBound::Following(1), FrameBound::Following(1)),
        },
    ];

    check_with_script_of_both_versions(
        |store, version| create_executor(calls.clone(), store, OptionalTables::None, version),
        r###"
- !barrier 1
- !chunk |2
      I T  I   i
    + 1 p1 100 10
    + 1 p1 101 16
    + 4 p2 200 20
- !chunk |2
      I T  I   i
    + 5 p1 102 18
    + 7 p2 201 22
    + 8 p3 300 33
# NOTE: no watermark message here, since watermark(1) was already received
- !barrier 2
- recovery
- !barrier 2
- !chunk |2
      I  T  I   i
    + 10 p1 103 13
    + 12 p2 202 28
    + 13 p3 301 39
- !barrier 3
"###,
        expect![[r#"
            - input: !barrier 1
              output:
              - !barrier 1
            - input: !chunk |-
                +---+---+----+-----+----+
                | + | 1 | p1 | 100 | 10 |
                | + | 1 | p1 | 101 | 16 |
                | + | 4 | p2 | 200 | 20 |
                +---+---+----+-----+----+
              output:
              - !chunk |-
                +---+---+----+-----+----+---+----+
                | + | 1 | p1 | 100 | 10 |   | 16 |
                +---+---+----+-----+----+---+----+
            - input: !chunk |-
                +---+---+----+-----+----+
                | + | 5 | p1 | 102 | 18 |
                | + | 7 | p2 | 201 | 22 |
                | + | 8 | p3 | 300 | 33 |
                +---+---+----+-----+----+
              output:
              - !chunk |-
                +---+---+----+-----+----+----+----+
                | + | 1 | p1 | 101 | 16 | 10 | 18 |
                | + | 4 | p2 | 200 | 20 |    | 22 |
                +---+---+----+-----+----+----+----+
            - input: !barrier 2
              output:
              - !barrier 2
            - input: recovery
              output: []
            - input: !barrier 2
              output:
              - !barrier 2
            - input: !chunk |-
                +---+----+----+-----+----+
                | + | 10 | p1 | 103 | 13 |
                | + | 12 | p2 | 202 | 28 |
                | + | 13 | p3 | 301 | 39 |
                +---+----+----+-----+----+
              output:
              - !chunk |-
                +---+---+----+-----+----+----+----+
                | + | 5 | p1 | 102 | 18 | 16 | 13 |
                | + | 7 | p2 | 201 | 22 | 20 | 28 |
                | + | 8 | p3 | 300 | 33 |    | 39 |
                +---+---+----+-----+----+----+----+
            - input: !barrier 3
              output:
              - !barrier 3
        "#]],
    )
    .await;
}

/// The input stream key `order key | pk` shares the order key with the state table pk, so evicting
/// row `(1, p1, 0)` must not delete row `(1, p1, 1)`, whose pk equals the order key.
#[tokio::test]
async fn test_over_window_stream_key_overlapping_order_key() {
    let calls = vec![
        // lead(x, 1)
        WindowFuncCall {
            kind: WindowFuncKind::Aggregate(PbAggKind::FirstValue.into()),
            return_type: DataType::Int32,
            args: AggArgs::from_iter([(DataType::Int32, 3)]),
            ignore_nulls: false,
            frame: Frame::rows(FrameBound::Following(1), FrameBound::Following(1)),
        },
    ];

    check_with_script_of_both_versions(
        |store, version| {
            create_executor_with_options(
                calls.clone(),
                store,
                ExecutorOptions {
                    input_stream_key: vec![0, 2],
                    ..ExecutorOptions::new(OptionalTables::None, version)
                },
            )
        },
        r###"
- !barrier 1
- !chunk |2
      I T  I i
    + 1 p1 0 10
    + 1 p1 1 20
- !barrier 2
- recovery
- !barrier 3
- !chunk |2
      I T  I i
    + 2 p1 2 30
- !barrier 4
"###,
        expect![[r#"
            - input: !barrier 1
              output:
              - !barrier 1
            - input: !chunk |-
                +---+---+----+---+----+
                | + | 1 | p1 | 0 | 10 |
                | + | 1 | p1 | 1 | 20 |
                +---+---+----+---+----+
              output:
              - !chunk |-
                +---+---+----+---+----+----+
                | + | 1 | p1 | 0 | 10 | 20 |
                +---+---+----+---+----+----+
            - input: !barrier 2
              output:
              - !barrier 2
            - input: recovery
              output: []
            - input: !barrier 3
              output:
              - !barrier 3
            - input: !chunk |-
                +---+---+----+---+----+
                | + | 2 | p1 | 2 | 30 |
                +---+---+----+---+----+
              output:
              - !chunk |-
                +---+---+----+---+----+----+
                | + | 1 | p1 | 1 | 20 | 30 |
                +---+---+----+---+----+----+
            - input: !barrier 4
              output:
              - !barrier 4
        "#]],
    )
    .await;
}

#[tokio::test]
async fn test_over_window_aggregate() {
    let calls = vec![WindowFuncCall {
        kind: WindowFuncKind::Aggregate(PbAggKind::Sum.into()),
        return_type: DataType::Int64,
        args: AggArgs::from_iter([(DataType::Int32, 3)]),
        ignore_nulls: false,
        frame: Frame::rows(FrameBound::Preceding(1), FrameBound::Following(1)),
    }];

    check_with_script_of_both_versions(
        |store, version| create_executor(calls.clone(), store, OptionalTables::None, version),
        r###"
        - !barrier 1
        - !chunk |2
              I T  I   i
            + 1 p1 100 10
            + 1 p1 101 16
            + 4 p1 102 20
        - !chunk |2
              I T  I   i
            + 2 p1 103 30
            + 6 p1 104 11
        "###,
        expect![[r#"
            - input: !barrier 1
              output:
              - !barrier 1
            - input: !chunk |-
                +---+---+----+-----+----+
                | + | 1 | p1 | 100 | 10 |
                | + | 1 | p1 | 101 | 16 |
                | + | 4 | p1 | 102 | 20 |
                +---+---+----+-----+----+
              output:
              - !chunk |-
                +---+---+----+-----+----+----+
                | + | 1 | p1 | 100 | 10 | 26 |
                | + | 1 | p1 | 101 | 16 | 46 |
                +---+---+----+-----+----+----+
            - input: !chunk |-
                +---+---+----+-----+----+
                | + | 2 | p1 | 103 | 30 |
                | + | 6 | p1 | 104 | 11 |
                +---+---+----+-----+----+
              output:
              - !chunk |-
                +---+---+----+-----+----+----+
                | + | 4 | p1 | 102 | 20 | 66 |
                | + | 2 | p1 | 103 | 30 | 61 |
                +---+---+----+-----+----+----+
        "#]],
    )
    .await;
}

/// Test EOWC `row_number` recovery continuity.
/// After recovery, ranks should continue from where they left off, not restart from 1.
#[tokio::test]
async fn test_over_window_row_number_recovery() {
    let calls = vec![WindowFuncCall {
        kind: WindowFuncKind::RowNumber,
        return_type: DataType::Int64,
        args: AggArgs::default(),
        ignore_nulls: false,
        frame: Frame::rows(FrameBound::CurrentRow, FrameBound::CurrentRow),
    }];

    check_with_script_of_both_versions(
        |store, version| {
            create_executor(
                calls.clone(),
                store,
                OptionalTables::IntermediateState,
                version,
            )
        },
        r###"
- !barrier 1
- !chunk |2
      I T  I   i
    + 1 p1 100 10
    + 2 p1 101 20
    + 3 p1 102 30
- !barrier 2
- recovery
- !barrier 2
- !chunk |2
      I T  I   i
    + 4 p1 103 40
    + 5 p1 104 50
- !barrier 3
"###,
        expect![[r#"
            - input: !barrier 1
              output:
              - !barrier 1
            - input: !chunk |-
                +---+---+----+-----+----+
                | + | 1 | p1 | 100 | 10 |
                | + | 2 | p1 | 101 | 20 |
                | + | 3 | p1 | 102 | 30 |
                +---+---+----+-----+----+
              output:
              - !chunk |-
                +---+---+----+-----+----+---+
                | + | 1 | p1 | 100 | 10 | 1 |
                | + | 2 | p1 | 101 | 20 | 2 |
                | + | 3 | p1 | 102 | 30 | 3 |
                +---+---+----+-----+----+---+
            - input: !barrier 2
              output:
              - !barrier 2
            - input: recovery
              output: []
            - input: !barrier 2
              output:
              - !barrier 2
            - input: !chunk |-
                +---+---+----+-----+----+
                | + | 4 | p1 | 103 | 40 |
                | + | 5 | p1 | 104 | 50 |
                +---+---+----+-----+----+
              output:
              - !chunk |-
                +---+---+----+-----+----+---+
                | + | 4 | p1 | 103 | 40 | 4 |
                | + | 5 | p1 | 104 | 50 | 5 |
                +---+---+----+-----+----+---+
            - input: !barrier 3
              output:
              - !barrier 3
        "#]],
    )
    .await;
}

/// Test EOWC with multiple numbering functions (`row_number` + `rank`) in one executor.
/// Each function should have its own separate snapshot.
#[tokio::test]
async fn test_over_window_multiple_numbering_functions() {
    let calls = vec![
        // row_number()
        WindowFuncCall {
            kind: WindowFuncKind::RowNumber,
            return_type: DataType::Int64,
            args: AggArgs::default(),
            ignore_nulls: false,
            frame: Frame::rows(FrameBound::CurrentRow, FrameBound::CurrentRow),
        },
        // rank()
        WindowFuncCall {
            kind: WindowFuncKind::Rank,
            return_type: DataType::Int64,
            args: AggArgs::default(),
            ignore_nulls: false,
            frame: Frame::rows(FrameBound::CurrentRow, FrameBound::CurrentRow),
        },
    ];

    check_with_script_of_both_versions(
        |store, version| {
            create_executor(
                calls.clone(),
                store,
                OptionalTables::IntermediateState,
                version,
            )
        },
        r###"
- !barrier 1
- !chunk |2
      I T  I   i
    + 1 p1 100 10
    + 1 p1 101 20
    + 2 p1 102 30
- !barrier 2
- recovery
- !barrier 2
- !chunk |2
      I T  I   i
    + 3 p1 103 40
    + 3 p1 104 50
- !barrier 3
"###,
        expect![[r#"
            - input: !barrier 1
              output:
              - !barrier 1
            - input: !chunk |-
                +---+---+----+-----+----+
                | + | 1 | p1 | 100 | 10 |
                | + | 1 | p1 | 101 | 20 |
                | + | 2 | p1 | 102 | 30 |
                +---+---+----+-----+----+
              output:
              - !chunk |-
                +---+---+----+-----+----+---+---+
                | + | 1 | p1 | 100 | 10 | 1 | 1 |
                | + | 1 | p1 | 101 | 20 | 2 | 1 |
                | + | 2 | p1 | 102 | 30 | 3 | 3 |
                +---+---+----+-----+----+---+---+
            - input: !barrier 2
              output:
              - !barrier 2
            - input: recovery
              output: []
            - input: !barrier 2
              output:
              - !barrier 2
            - input: !chunk |-
                +---+---+----+-----+----+
                | + | 3 | p1 | 103 | 40 |
                | + | 3 | p1 | 104 | 50 |
                +---+---+----+-----+----+
              output:
              - !chunk |-
                +---+---+----+-----+----+---+---+
                | + | 3 | p1 | 103 | 40 | 4 | 4 |
                | + | 3 | p1 | 104 | 50 | 5 | 4 |
                +---+---+----+-----+----+---+---+
            - input: !barrier 3
              output:
              - !barrier 3
        "#]],
    )
    .await;
}

/// Test EOWC with interleaved partitions in one chunk.
/// Per-partition snapshots should remain isolated.
#[tokio::test]
async fn test_over_window_interleaved_partitions() {
    let calls = vec![WindowFuncCall {
        kind: WindowFuncKind::RowNumber,
        return_type: DataType::Int64,
        args: AggArgs::default(),
        ignore_nulls: false,
        frame: Frame::rows(FrameBound::CurrentRow, FrameBound::CurrentRow),
    }];

    check_with_script_of_both_versions(
        |store, version| {
            create_executor(
                calls.clone(),
                store,
                OptionalTables::IntermediateState,
                version,
            )
        },
        r###"
- !barrier 1
- !chunk |2
      I T  I   i
    + 1 p1 100 10
    + 1 p2 200 20
    + 2 p1 101 11
    + 2 p2 201 21
- !barrier 2
- recovery
- !barrier 2
- !chunk |2
      I T  I   i
    + 3 p1 102 12
    + 3 p2 202 22
- !barrier 3
"###,
        expect![[r#"
            - input: !barrier 1
              output:
              - !barrier 1
            - input: !chunk |-
                +---+---+----+-----+----+
                | + | 1 | p1 | 100 | 10 |
                | + | 1 | p2 | 200 | 20 |
                | + | 2 | p1 | 101 | 11 |
                | + | 2 | p2 | 201 | 21 |
                +---+---+----+-----+----+
              output:
              - !chunk |-
                +---+---+----+-----+----+---+
                | + | 1 | p1 | 100 | 10 | 1 |
                | + | 1 | p2 | 200 | 20 | 1 |
                | + | 2 | p1 | 101 | 11 | 2 |
                | + | 2 | p2 | 201 | 21 | 2 |
                +---+---+----+-----+----+---+
            - input: !barrier 2
              output:
              - !barrier 2
            - input: recovery
              output: []
            - input: !barrier 2
              output:
              - !barrier 2
            - input: !chunk |-
                +---+---+----+-----+----+
                | + | 3 | p1 | 102 | 12 |
                | + | 3 | p2 | 202 | 22 |
                +---+---+----+-----+----+
              output:
              - !chunk |-
                +---+---+----+-----+----+---+
                | + | 3 | p1 | 102 | 12 | 3 |
                | + | 3 | p2 | 202 | 22 | 3 |
                +---+---+----+-----+----+---+
            - input: !barrier 3
              output:
              - !barrier 3
        "#]],
    )
    .await;
}

/// Test EOWC with mixed numbering + aggregate windows.
/// Eviction hints should be merged correctly.
#[tokio::test]
async fn test_over_window_mixed_numbering_and_aggregate() {
    let calls = vec![
        // row_number()
        WindowFuncCall {
            kind: WindowFuncKind::RowNumber,
            return_type: DataType::Int64,
            args: AggArgs::default(),
            ignore_nulls: false,
            frame: Frame::rows(FrameBound::CurrentRow, FrameBound::CurrentRow),
        },
        // sum(x) ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
        WindowFuncCall {
            kind: WindowFuncKind::Aggregate(PbAggKind::Sum.into()),
            return_type: DataType::Int64,
            args: AggArgs::from_iter([(DataType::Int32, 3)]),
            ignore_nulls: false,
            frame: Frame::rows(FrameBound::Preceding(1), FrameBound::CurrentRow),
        },
    ];

    check_with_script_of_both_versions(
        |store, version| {
            create_executor(
                calls.clone(),
                store,
                OptionalTables::IntermediateState,
                version,
            )
        },
        r###"
- !barrier 1
- !chunk |2
      I T  I   i
    + 1 p1 100 10
    + 2 p1 101 20
    + 3 p1 102 30
- !barrier 2
- recovery
- !barrier 2
- !chunk |2
      I T  I   i
    + 4 p1 103 40
- !barrier 3
"###,
        expect![[r#"
            - input: !barrier 1
              output:
              - !barrier 1
            - input: !chunk |-
                +---+---+----+-----+----+
                | + | 1 | p1 | 100 | 10 |
                | + | 2 | p1 | 101 | 20 |
                | + | 3 | p1 | 102 | 30 |
                +---+---+----+-----+----+
              output:
              - !chunk |-
                +---+---+----+-----+----+---+----+
                | + | 1 | p1 | 100 | 10 | 1 | 10 |
                | + | 2 | p1 | 101 | 20 | 2 | 30 |
                | + | 3 | p1 | 102 | 30 | 3 | 50 |
                +---+---+----+-----+----+---+----+
            - input: !barrier 2
              output:
              - !barrier 2
            - input: recovery
              output: []
            - input: !barrier 2
              output:
              - !barrier 2
            - input: !chunk |-
                +---+---+----+-----+----+
                | + | 4 | p1 | 103 | 40 |
                +---+---+----+-----+----+
              output:
              - !chunk |-
                +---+---+----+-----+----+---+----+
                | + | 4 | p1 | 103 | 40 | 4 | 70 |
                +---+---+----+-----+----+---+----+
            - input: !barrier 3
              output:
              - !barrier 3
        "#]],
    )
    .await;
}

/// Test EOWC without `intermediate_state_table` behaves exactly as before (legacy compatibility).
/// Eviction hint should be `CannotEvict` when `intermediate_state_table` is absent.
#[tokio::test]
async fn test_over_window_legacy_without_intermediate_state_table() {
    let store = MemoryStateStore::new();
    // Use row_number without intermediate state table (legacy mode)
    let calls = vec![WindowFuncCall {
        kind: WindowFuncKind::RowNumber,
        return_type: DataType::Int64,
        args: AggArgs::default(),
        ignore_nulls: false,
        frame: Frame::rows(FrameBound::CurrentRow, FrameBound::CurrentRow),
    }];

    check_with_script(
        || create_executor(calls.clone(), store.clone(), OptionalTables::None, LEGACY),
        r###"
- !barrier 1
- !chunk |2
      I T  I   i
    + 1 p1 100 10
    + 2 p1 101 20
- !barrier 2
- recovery
- !barrier 2
- !chunk |2
      I T  I   i
    + 3 p1 102 30
- !barrier 3
"###,
        expect![[r#"
            - input: !barrier 1
              output:
              - !barrier 1
            - input: !chunk |-
                +---+---+----+-----+----+
                | + | 1 | p1 | 100 | 10 |
                | + | 2 | p1 | 101 | 20 |
                +---+---+----+-----+----+
              output:
              - !chunk |-
                +---+---+----+-----+----+---+
                | + | 1 | p1 | 100 | 10 | 1 |
                | + | 2 | p1 | 101 | 20 | 2 |
                +---+---+----+-----+----+---+
            - input: !barrier 2
              output:
              - !barrier 2
            - input: recovery
              output: []
            - input: !barrier 2
              output:
              - !barrier 2
            - input: !chunk |-
                +---+---+----+-----+----+
                | + | 3 | p1 | 102 | 30 |
                +---+---+----+-----+----+
              output:
              - !chunk |-
                +---+---+----+-----+----+---+
                | + | 3 | p1 | 102 | 30 | 3 |
                +---+---+----+-----+----+---+
            - input: !barrier 3
              output:
              - !barrier 3
        "#]],
        SnapshotOptions::default(),
    )
    .await;
}

/// Sessions are closed by watermark without new rows, and the output rows retained for `lag` are
/// not output again after recovery.
#[tokio::test]
async fn test_session_closed_by_watermark() {
    let store = MemoryStateStore::new();
    let calls = vec![session_count(5), lag_or_lead(false)];

    check_with_script(
        || create_executor_with_watermark_trigger_table(calls.clone(), store.clone(), 1024),
        r###"
- !barrier 1
- !chunk |2
      I T  I   i
    + 1 p1 100 10
    + 3 p1 101 20
    + 4 p2 200 30
# p1 is closed at 3 + 5 = 8, p2 at 4 + 5 = 9.
- !watermark
  col_idx: 0
  val: '7'
- !watermark
  col_idx: 0
  val: '8'
- !barrier 2
- recovery
- !barrier 3
# 3 of p1 is retained for `lag` of 20, but must not be output again.
- !chunk |2
      I T  I   i
    + 20 p1 102 40
- !watermark
  col_idx: 0
  val: '9'
- !watermark
  col_idx: 0
  val: '25'
- !barrier 4
"###,
        expect![[r#"
            - input: !barrier 1
              output:
              - !barrier 1
            - input: !chunk |-
                +---+---+----+-----+----+
                | + | 1 | p1 | 100 | 10 |
                | + | 3 | p1 | 101 | 20 |
                | + | 4 | p2 | 200 | 30 |
                +---+---+----+-----+----+
              output: []
            - input: !watermark
                col_idx: 0
                val: '7'
              output: []
            - input: !watermark
                col_idx: 0
                val: '8'
              output:
              - !chunk |-
                +---+---+----+-----+----+---+----+
                | + | 1 | p1 | 100 | 10 | 2 |    |
                | + | 3 | p1 | 101 | 20 | 2 | 10 |
                +---+---+----+-----+----+---+----+
            - input: !barrier 2
              output:
              - !barrier 2
            - input: recovery
              output: []
            - input: !barrier 3
              output:
              - !barrier 3
            - input: !chunk |-
                +---+----+----+-----+----+
                | + | 20 | p1 | 102 | 40 |
                +---+----+----+-----+----+
              output: []
            - input: !watermark
                col_idx: 0
                val: '9'
              output:
              - !chunk |-
                +---+---+----+-----+----+---+---+
                | + | 4 | p2 | 200 | 30 | 1 |   |
                +---+---+----+-----+----+---+---+
            - input: !watermark
                col_idx: 0
                val: '25'
              output:
              - !chunk |-
                +---+----+----+-----+----+---+----+
                | + | 20 | p1 | 102 | 40 | 1 | 20 |
                +---+----+----+-----+----+---+----+
            - input: !barrier 4
              output:
              - !barrier 4
        "#]],
        SnapshotOptions::default(),
    )
    .await;
}

/// The output progress and the snapshot of `row_number` are both persisted, so that neither the
/// rows retained for `lag` nor the row numbers are output again after recovery.
#[tokio::test]
async fn test_numbering_closed_by_watermark() {
    let store = MemoryStateStore::new();
    let calls = vec![
        WindowFuncCall {
            kind: WindowFuncKind::RowNumber,
            return_type: DataType::Int64,
            args: AggArgs::default(),
            ignore_nulls: false,
            frame: Frame::rows(FrameBound::CurrentRow, FrameBound::CurrentRow),
        },
        session_count(5),
        lag_or_lead(false),
    ];

    check_with_script(
        || create_executor_with_watermark_trigger_table(calls.clone(), store.clone(), 1024),
        r###"
- !barrier 1
- !chunk |2
      I T  I   i
    + 1 p1 100 10
    + 3 p1 101 20
- !watermark
  col_idx: 0
  val: '8'
- !barrier 2
- recovery
- !barrier 3
- !chunk |2
      I T  I   i
    + 20 p1 102 30
- !watermark
  col_idx: 0
  val: '25'
- !barrier 4
"###,
        expect![[r#"
            - input: !barrier 1
              output:
              - !barrier 1
            - input: !chunk |-
                +---+---+----+-----+----+
                | + | 1 | p1 | 100 | 10 |
                | + | 3 | p1 | 101 | 20 |
                +---+---+----+-----+----+
              output: []
            - input: !watermark
                col_idx: 0
                val: '8'
              output:
              - !chunk |-
                +---+---+----+-----+----+---+---+----+
                | + | 1 | p1 | 100 | 10 | 1 | 2 |    |
                | + | 3 | p1 | 101 | 20 | 2 | 2 | 10 |
                +---+---+----+-----+----+---+---+----+
            - input: !barrier 2
              output:
              - !barrier 2
            - input: recovery
              output: []
            - input: !barrier 3
              output:
              - !barrier 3
            - input: !chunk |-
                +---+----+----+-----+----+
                | + | 20 | p1 | 102 | 30 |
                +---+----+----+-----+----+
              output: []
            - input: !watermark
                col_idx: 0
                val: '25'
              output:
              - !chunk |-
                +---+----+----+-----+----+---+---+----+
                | + | 20 | p1 | 102 | 30 | 3 | 1 | 20 |
                +---+----+----+-----+----+---+---+----+
            - input: !barrier 4
              output:
              - !barrier 4
        "#]],
        SnapshotOptions::default(),
    )
    .await;
}

/// Rows equal to the end of a `RANGE` frame may still come, so the frame is closed only by a
/// greater watermark.
#[tokio::test]
async fn test_range_closed_by_watermark() {
    let store = MemoryStateStore::new();
    let calls = vec![range_sum(FrameBound::CurrentRow, FrameBound::Following(2))];

    check_with_script(
        || create_executor_with_watermark_trigger_table(calls.clone(), store.clone(), 1024),
        r###"
- !barrier 1
- !chunk |2
      I T  I   i
    + 1 p1 100 10
    + 1 p1 101 20
    + 2 p1 102 30
- !watermark
  col_idx: 0
  val: '3'
- !barrier 2
- recovery
- !barrier 3
- !watermark
  col_idx: 0
  val: '4'
- !watermark
  col_idx: 0
  val: '5'
- !barrier 4
"###,
        expect![[r#"
            - input: !barrier 1
              output:
              - !barrier 1
            - input: !chunk |-
                +---+---+----+-----+----+
                | + | 1 | p1 | 100 | 10 |
                | + | 1 | p1 | 101 | 20 |
                | + | 2 | p1 | 102 | 30 |
                +---+---+----+-----+----+
              output: []
            - input: !watermark
                col_idx: 0
                val: '3'
              output: []
            - input: !barrier 2
              output:
              - !barrier 2
            - input: recovery
              output: []
            - input: !barrier 3
              output:
              - !barrier 3
            - input: !watermark
                col_idx: 0
                val: '4'
              output:
              - !chunk |-
                +---+---+----+-----+----+----+
                | + | 1 | p1 | 100 | 10 | 60 |
                | + | 1 | p1 | 101 | 20 | 60 |
                +---+---+----+-----+----+----+
            - input: !watermark
                col_idx: 0
                val: '5'
              output:
              - !chunk |-
                +---+---+----+-----+----+----+
                | + | 2 | p1 | 102 | 30 | 30 |
                +---+---+----+-----+----+----+
            - input: !barrier 4
              output:
              - !barrier 4
        "#]],
        SnapshotOptions::default(),
    )
    .await;
}

/// A window of `lead` can only be closed by new rows, even if the session is closed by watermark.
#[tokio::test]
async fn test_closed_by_watermark_and_rows() {
    let store = MemoryStateStore::new();
    let calls = vec![session_count(5), lag_or_lead(true)];

    check_with_script(
        || create_executor_with_watermark_trigger_table(calls.clone(), store.clone(), 1024),
        r###"
- !barrier 1
- !chunk |2
      I T  I   i
    + 1 p1 100 10
    + 3 p1 101 20
- !watermark
  col_idx: 0
  val: '10'
- !watermark
  col_idx: 0
  val: '20'
- !chunk |2
      I T  I   i
    + 30 p1 102 30
- !watermark
  col_idx: 0
  val: '40'
- !barrier 2
"###,
        expect![[r#"
            - input: !barrier 1
              output:
              - !barrier 1
            - input: !chunk |-
                +---+---+----+-----+----+
                | + | 1 | p1 | 100 | 10 |
                | + | 3 | p1 | 101 | 20 |
                +---+---+----+-----+----+
              output: []
            - input: !watermark
                col_idx: 0
                val: '10'
              output:
              - !chunk |-
                +---+---+----+-----+----+---+----+
                | + | 1 | p1 | 100 | 10 | 2 | 20 |
                +---+---+----+-----+----+---+----+
            - input: !watermark
                col_idx: 0
                val: '20'
              output: []
            - input: !chunk |-
                +---+----+----+-----+----+
                | + | 30 | p1 | 102 | 30 |
                +---+----+----+-----+----+
              output:
              - !chunk |-
                +---+---+----+-----+----+---+----+
                | + | 3 | p1 | 101 | 20 | 2 | 30 |
                +---+---+----+-----+----+---+----+
            - input: !watermark
                col_idx: 0
                val: '40'
              output: []
            - input: !barrier 2
              output:
              - !barrier 2
        "#]],
        SnapshotOptions::default(),
    )
    .await;
}

/// The rows closed by a watermark are output in chunks of bounded size.
#[tokio::test]
async fn test_closed_by_watermark_in_chunks() {
    let store = MemoryStateStore::new();
    let calls = vec![session_count(5)];

    check_with_script(
        || create_executor_with_watermark_trigger_table(calls.clone(), store.clone(), 2),
        r###"
- !barrier 1
- !chunk |2
      I T  I   i
    + 1 p1 100 10
    + 1 p2 200 20
    + 1 p3 300 30
    + 2 p4 400 40
    + 2 p5 500 50
- !watermark
  col_idx: 0
  val: '7'
- !barrier 2
"###,
        expect![[r#"
            - input: !barrier 1
              output:
              - !barrier 1
            - input: !chunk |-
                +---+---+----+-----+----+
                | + | 1 | p1 | 100 | 10 |
                | + | 1 | p2 | 200 | 20 |
                | + | 1 | p3 | 300 | 30 |
                | + | 2 | p4 | 400 | 40 |
                | + | 2 | p5 | 500 | 50 |
                +---+---+----+-----+----+
              output: []
            - input: !watermark
                col_idx: 0
                val: '7'
              output:
              - !chunk |-
                +---+---+----+-----+----+---+
                | + | 1 | p1 | 100 | 10 | 1 |
                | + | 1 | p2 | 200 | 20 | 1 |
                +---+---+----+-----+----+---+
              - !chunk |-
                +---+---+----+-----+----+---+
                | + | 1 | p3 | 300 | 30 | 1 |
                | + | 2 | p4 | 400 | 40 | 1 |
                +---+---+----+-----+----+---+
              - !chunk |-
                +---+---+----+-----+----+---+
                | + | 2 | p5 | 500 | 50 | 1 |
                +---+---+----+-----+----+---+
            - input: !barrier 2
              output:
              - !barrier 2
        "#]],
        SnapshotOptions::default(),
    )
    .await;
}

/// A row output by watermark before the next row arrives is retained, and must be evicted once the
/// next row arrives, even if the partition is reloaded in between.
#[tokio::test]
async fn test_closed_by_watermark_and_reloaded() {
    let store = MemoryStateStore::new();
    let calls = vec![range_sum(FrameBound::CurrentRow, FrameBound::Following(2))];

    check_with_script(
        || create_executor_with_watermark_trigger_table(calls.clone(), store.clone(), 1024),
        r###"
- !barrier 1
- !chunk |2
      I T  I   i
    + 0 p1 100 1
- !watermark
  col_idx: 0
  val: '3'
- !chunk |2
      I  T  I   i
    + 10 p1 101 2
- !watermark
  col_idx: 0
  val: '11'
- !barrier 2
- recovery
- !barrier 3
- !watermark
  col_idx: 0
  val: '13'
- !barrier 4
"###,
        expect![[r#"
            - input: !barrier 1
              output:
              - !barrier 1
            - input: !chunk |-
                +---+---+----+-----+---+
                | + | 0 | p1 | 100 | 1 |
                +---+---+----+-----+---+
              output: []
            - input: !watermark
                col_idx: 0
                val: '3'
              output:
              - !chunk |-
                +---+---+----+-----+---+---+
                | + | 0 | p1 | 100 | 1 | 1 |
                +---+---+----+-----+---+---+
            - input: !chunk |-
                +---+----+----+-----+---+
                | + | 10 | p1 | 101 | 2 |
                +---+----+----+-----+---+
              output: []
            - input: !watermark
                col_idx: 0
                val: '11'
              output: []
            - input: !barrier 2
              output:
              - !barrier 2
            - input: recovery
              output: []
            - input: !barrier 3
              output:
              - !barrier 3
            - input: !watermark
                col_idx: 0
                val: '13'
              output:
              - !chunk |-
                +---+----+----+-----+---+---+
                | + | 10 | p1 | 101 | 2 | 2 |
                +---+----+----+-----+---+---+
            - input: !barrier 4
              output:
              - !barrier 4
        "#]],
        SnapshotOptions::default(),
    )
    .await;

    // Only the last row is retained, for the windows of future rows.
    assert_eq!(
        state_table_rows(store).await,
        vec![OwnedRow::new(vec![
            Some(ScalarImpl::Int64(10)),
            Some(ScalarImpl::Utf8("p1".into())),
            Some(ScalarImpl::Int64(101)),
            Some(ScalarImpl::Int32(2)),
        ])]
    );
}

/// A watermark closes the windows of more partitions than the trigger cache holds.
#[tokio::test]
async fn test_closed_by_watermark_many_partitions() {
    let store = MemoryStateStore::new();
    let (mut tx, mut executor) =
        create_executor_with_watermark_trigger_table(vec![session_count(1)], store.clone(), 1024)
            .await;
    let num_partitions = 3000;

    tx.push_barrier(test_epoch(1), false);
    executor.expect_barrier().await;
    let rows = (0..num_partitions)
        .map(|i| format!("+ 1 p{i} {i} 1"))
        .collect::<Vec<_>>()
        .join("\n");
    tx.push_chunk(StreamChunk::from_pretty(&format!(" I T I i\n{rows}")));
    tx.push_watermark(0, DataType::Int64, ScalarImpl::Int64(2));
    tx.push_barrier(test_epoch(2), false);

    let mut num_output_rows = 0;
    loop {
        match executor.next().await.unwrap().unwrap() {
            Message::Chunk(chunk) => num_output_rows += chunk.cardinality(),
            Message::Barrier(_) => break,
            Message::Watermark(_) => {}
        }
    }
    assert_eq!(num_output_rows, num_partitions);
    // The rows, the output progress and the triggers of the closed sessions are all cleaned.
    assert!(state_table_rows(store.clone()).await.is_empty());
    assert!(
        table_rows(
            &intermediate_state_table_catalog(1, true, false),
            store.clone(),
            None
        )
        .await
        .is_empty()
    );
    assert!(
        table_rows(&watermark_trigger_table_catalog(false), store, None)
            .await
            .is_empty()
    );
}

/// Plans created before close windows only by later rows, judged conservatively, ignoring
/// watermarks, and skip the output rows on recovery by that.
#[tokio::test]
async fn test_legacy_plan_ignores_watermark() {
    // The last row waits for the next row, though the frame ends before it.
    let store = MemoryStateStore::new();
    let calls = vec![range_sum(
        FrameBound::Preceding(10),
        FrameBound::Preceding(5),
    )];
    check_with_script(
        || create_executor(calls.clone(), store.clone(), OptionalTables::None, LEGACY),
        r###"
- !barrier 1
- !chunk |2
      I T  I   i
    + 1 p1 100 10
- !watermark
  col_idx: 0
  val: '15'
- !barrier 2
- recovery
- !barrier 3
- !chunk |2
      I  T  I   i
    + 20 p1 101 20
- !barrier 4
"###,
        expect![[r#"
            - input: !barrier 1
              output:
              - !barrier 1
            - input: !chunk |-
                +---+---+----+-----+----+
                | + | 1 | p1 | 100 | 10 |
                +---+---+----+-----+----+
              output: []
            - input: !watermark
                col_idx: 0
                val: '15'
              output: []
            - input: !barrier 2
              output:
              - !barrier 2
            - input: recovery
              output: []
            - input: !barrier 3
              output:
              - !barrier 3
            - input: !chunk |-
                +---+----+----+-----+----+
                | + | 20 | p1 | 101 | 20 |
                +---+----+----+-----+----+
              output:
              - !chunk |-
                +---+---+----+-----+----+---+
                | + | 1 | p1 | 100 | 10 |   |
                +---+---+----+-----+----+---+
            - input: !barrier 4
              output:
              - !barrier 4
        "#]],
        SnapshotOptions::default(),
    )
    .await;

    // The session is closed only by a row of the next session.
    let store = MemoryStateStore::new();
    let calls = vec![session_count(5), lag_or_lead(false)];
    check_with_script(
        || create_executor(calls.clone(), store.clone(), OptionalTables::None, LEGACY),
        r###"
- !barrier 1
- !chunk |2
      I T  I   i
    + 1 p1 100 10
    + 3 p1 101 20
- !watermark
  col_idx: 0
  val: '15'
- !barrier 2
- recovery
- !barrier 3
- !chunk |2
      I  T  I   i
    + 20 p1 102 30
- !chunk |2
      I  T  I   i
    + 40 p1 103 40
- !barrier 4
"###,
        expect![[r#"
            - input: !barrier 1
              output:
              - !barrier 1
            - input: !chunk |-
                +---+---+----+-----+----+
                | + | 1 | p1 | 100 | 10 |
                | + | 3 | p1 | 101 | 20 |
                +---+---+----+-----+----+
              output: []
            - input: !watermark
                col_idx: 0
                val: '15'
              output: []
            - input: !barrier 2
              output:
              - !barrier 2
            - input: recovery
              output: []
            - input: !barrier 3
              output:
              - !barrier 3
            - input: !chunk |-
                +---+----+----+-----+----+
                | + | 20 | p1 | 102 | 30 |
                +---+----+----+-----+----+
              output:
              - !chunk |-
                +---+---+----+-----+----+---+----+
                | + | 1 | p1 | 100 | 10 | 2 |    |
                | + | 3 | p1 | 101 | 20 | 2 | 10 |
                +---+---+----+-----+----+---+----+
            - input: !chunk |-
                +---+----+----+-----+----+
                | + | 40 | p1 | 103 | 40 |
                +---+----+----+-----+----+
              output:
              - !chunk |-
                +---+----+----+-----+----+---+----+
                | + | 20 | p1 | 102 | 30 | 1 | 20 |
                +---+----+----+-----+----+---+----+
            - input: !barrier 4
              output:
              - !barrier 4
        "#]],
        SnapshotOptions::default(),
    )
    .await;
}

/// A frame ending before the current row is closed once the row arrives. Without the output
/// progress, the output rows are skipped on recovery by the readiness, and the rows not needed any
/// more are evicted.
#[tokio::test]
async fn test_range_closed_by_input() {
    let store = MemoryStateStore::new();
    let calls = vec![range_sum(
        FrameBound::Preceding(10),
        FrameBound::Preceding(5),
    )];

    check_with_script(
        || create_executor(calls.clone(), store.clone(), OptionalTables::None, LATEST),
        r###"
- !barrier 1
- !chunk |2
      I T  I   i
    + 1 p1 100 10
    + 4 p1 101 20
- !barrier 2
- recovery
- !barrier 3
- !chunk |2
      I  T  I   i
    + 12 p1 102 30
    + 13 p1 103 40
- !barrier 4
"###,
        expect![[r#"
            - input: !barrier 1
              output:
              - !barrier 1
            - input: !chunk |-
                +---+---+----+-----+----+
                | + | 1 | p1 | 100 | 10 |
                | + | 4 | p1 | 101 | 20 |
                +---+---+----+-----+----+
              output:
              - !chunk |-
                +---+---+----+-----+----+---+
                | + | 1 | p1 | 100 | 10 |   |
                | + | 4 | p1 | 101 | 20 |   |
                +---+---+----+-----+----+---+
            - input: !barrier 2
              output:
              - !barrier 2
            - input: recovery
              output: []
            - input: !barrier 3
              output:
              - !barrier 3
            - input: !chunk |-
                +---+----+----+-----+----+
                | + | 12 | p1 | 102 | 30 |
                | + | 13 | p1 | 103 | 40 |
                +---+----+----+-----+----+
              output:
              - !chunk |-
                +---+----+----+-----+----+----+
                | + | 12 | p1 | 102 | 30 | 20 |
                | + | 13 | p1 | 103 | 40 | 20 |
                +---+----+----+-----+----+----+
            - input: !barrier 4
              output:
              - !barrier 4
        "#]],
        SnapshotOptions::default(),
    )
    .await;

    let row = |order: i64, pk: i64, x: i32| {
        OwnedRow::new(vec![
            Some(ScalarImpl::Int64(order)),
            Some(ScalarImpl::Utf8("p1".into())),
            Some(ScalarImpl::Int64(pk)),
            Some(ScalarImpl::Int32(x)),
        ])
    };
    assert_eq!(
        state_table_rows(store).await,
        vec![row(4, 101, 20), row(12, 102, 30), row(13, 103, 40)]
    );
}

/// The partitions closed by a watermark are loaded from storage a chunk at a time, skipping the
/// output rows retained for `lag`.
#[tokio::test]
async fn test_closed_by_watermark_cold_partitions() {
    let store = MemoryStateStore::new();
    let calls = vec![session_count(5), lag_or_lead(false)];

    check_with_script(
        || create_executor_with_watermark_trigger_table(calls.clone(), store.clone(), 2),
        r###"
- !barrier 1
- !chunk |2
      I T  I   i
    + 1 p1 100 10
    + 1 p2 200 20
    + 1 p3 300 30
    + 2 p4 400 40
    + 2 p5 500 50
- !watermark
  col_idx: 0
  val: '7'
- !barrier 2
- recovery
- !barrier 3
- !chunk |2
      I  T  I   i
    + 20 p1 101 11
    + 20 p2 201 21
    + 20 p3 301 31
    + 20 p4 401 41
    + 20 p5 501 51
- !barrier 4
- recovery
- !barrier 5
- !watermark
  col_idx: 0
  val: '30'
- !barrier 6
"###,
        expect![[r#"
            - input: !barrier 1
              output:
              - !barrier 1
            - input: !chunk |-
                +---+---+----+-----+----+
                | + | 1 | p1 | 100 | 10 |
                | + | 1 | p2 | 200 | 20 |
                | + | 1 | p3 | 300 | 30 |
                | + | 2 | p4 | 400 | 40 |
                | + | 2 | p5 | 500 | 50 |
                +---+---+----+-----+----+
              output: []
            - input: !watermark
                col_idx: 0
                val: '7'
              output:
              - !chunk |-
                +---+---+----+-----+----+---+---+
                | + | 1 | p1 | 100 | 10 | 1 |   |
                | + | 1 | p2 | 200 | 20 | 1 |   |
                +---+---+----+-----+----+---+---+
              - !chunk |-
                +---+---+----+-----+----+---+---+
                | + | 1 | p3 | 300 | 30 | 1 |   |
                | + | 2 | p4 | 400 | 40 | 1 |   |
                +---+---+----+-----+----+---+---+
              - !chunk |-
                +---+---+----+-----+----+---+---+
                | + | 2 | p5 | 500 | 50 | 1 |   |
                +---+---+----+-----+----+---+---+
            - input: !barrier 2
              output:
              - !barrier 2
            - input: recovery
              output: []
            - input: !barrier 3
              output:
              - !barrier 3
            - input: !chunk |-
                +---+----+----+-----+----+
                | + | 20 | p1 | 101 | 11 |
                | + | 20 | p2 | 201 | 21 |
                | + | 20 | p3 | 301 | 31 |
                | + | 20 | p4 | 401 | 41 |
                | + | 20 | p5 | 501 | 51 |
                +---+----+----+-----+----+
              output: []
            - input: !barrier 4
              output:
              - !barrier 4
            - input: recovery
              output: []
            - input: !barrier 5
              output:
              - !barrier 5
            - input: !watermark
                col_idx: 0
                val: '30'
              output:
              - !chunk |-
                +---+----+----+-----+----+---+----+
                | + | 20 | p1 | 101 | 11 | 1 | 10 |
                | + | 20 | p2 | 201 | 21 | 1 | 20 |
                +---+----+----+-----+----+---+----+
              - !chunk |-
                +---+----+----+-----+----+---+----+
                | + | 20 | p3 | 301 | 31 | 1 | 30 |
                | + | 20 | p4 | 401 | 41 | 1 | 40 |
                +---+----+----+-----+----+---+----+
              - !chunk |-
                +---+----+----+-----+----+---+----+
                | + | 20 | p5 | 501 | 51 | 1 | 50 |
                +---+----+----+-----+----+---+----+
            - input: !barrier 6
              output:
              - !barrier 6
        "#]],
        SnapshotOptions::default(),
    )
    .await;

    // Only the last rows are retained for `lag`, and no trigger is left.
    assert_eq!(
        state_table_rows(store.clone())
            .await
            .iter()
            .map(|row| row.datum_at(0).unwrap().into_int64())
            .collect_vec(),
        vec![20; 5]
    );
    assert!(
        table_rows(&watermark_trigger_table_catalog(false), store, None)
            .await
            .is_empty()
    );
}

/// Evicting the cached partitions between the chunks of a watermark doesn't change the output.
#[tokio::test]
async fn test_closed_by_watermark_with_eviction() {
    let store = MemoryStateStore::new();
    let calls = vec![session_count(5), lag_or_lead(false)];

    check_with_script(
        || {
            create_executor_with_options(
                calls.clone(),
                store.clone(),
                ExecutorOptions {
                    chunk_size: 2,
                    evict_all: true,
                    ..ExecutorOptions::new(OptionalTables::WatermarkTrigger, LATEST)
                },
            )
        },
        r###"
- !barrier 1
- !chunk |2
      I T  I   i
    + 1 p1 100 10
    + 1 p2 200 20
    + 1 p3 300 30
    + 2 p4 400 40
    + 2 p5 500 50
- !watermark
  col_idx: 0
  val: '7'
- !chunk |2
      I  T  I   i
    + 20 p1 101 11
    + 20 p2 201 21
    + 20 p3 301 31
    + 20 p4 401 41
    + 20 p5 501 51
- !watermark
  col_idx: 0
  val: '30'
- !barrier 2
"###,
        expect![[r#"
            - input: !barrier 1
              output:
              - !barrier 1
            - input: !chunk |-
                +---+---+----+-----+----+
                | + | 1 | p1 | 100 | 10 |
                | + | 1 | p2 | 200 | 20 |
                | + | 1 | p3 | 300 | 30 |
                | + | 2 | p4 | 400 | 40 |
                | + | 2 | p5 | 500 | 50 |
                +---+---+----+-----+----+
              output: []
            - input: !watermark
                col_idx: 0
                val: '7'
              output:
              - !chunk |-
                +---+---+----+-----+----+---+---+
                | + | 1 | p1 | 100 | 10 | 1 |   |
                | + | 1 | p2 | 200 | 20 | 1 |   |
                +---+---+----+-----+----+---+---+
              - !chunk |-
                +---+---+----+-----+----+---+---+
                | + | 1 | p3 | 300 | 30 | 1 |   |
                | + | 2 | p4 | 400 | 40 | 1 |   |
                +---+---+----+-----+----+---+---+
              - !chunk |-
                +---+---+----+-----+----+---+---+
                | + | 2 | p5 | 500 | 50 | 1 |   |
                +---+---+----+-----+----+---+---+
            - input: !chunk |-
                +---+----+----+-----+----+
                | + | 20 | p1 | 101 | 11 |
                | + | 20 | p2 | 201 | 21 |
                | + | 20 | p3 | 301 | 31 |
                | + | 20 | p4 | 401 | 41 |
                | + | 20 | p5 | 501 | 51 |
                +---+----+----+-----+----+
              output: []
            - input: !watermark
                col_idx: 0
                val: '30'
              output:
              - !chunk |-
                +---+----+----+-----+----+---+----+
                | + | 20 | p1 | 101 | 11 | 1 | 10 |
                | + | 20 | p2 | 201 | 21 | 1 | 20 |
                +---+----+----+-----+----+---+----+
              - !chunk |-
                +---+----+----+-----+----+---+----+
                | + | 20 | p3 | 301 | 31 | 1 | 30 |
                | + | 20 | p4 | 401 | 41 | 1 | 40 |
                +---+----+----+-----+----+---+----+
              - !chunk |-
                +---+----+----+-----+----+---+----+
                | + | 20 | p5 | 501 | 51 | 1 | 50 |
                +---+----+----+-----+----+---+----+
            - input: !barrier 2
              output:
              - !barrier 2
        "#]],
        SnapshotOptions::default(),
    )
    .await;
}

/// After the vnodes of the executor change, only the partitions in the vnodes it owns are closed.
#[tokio::test]
async fn test_closed_by_watermark_after_vnode_update() {
    let vnode_count = VirtualNode::COUNT_FOR_TEST;
    let partitions = (0..8).map(|i| format!("p{i}")).collect_vec();
    let vnode_of = |partition: &str| {
        VirtualNode::compute_row(
            row::once(Some(ScalarImpl::Utf8(partition.into()))),
            &[0],
            vnode_count,
        )
    };
    // Keep the vnodes of the first half of the partitions.
    let mut builder = BitmapBuilder::zeroed(vnode_count);
    for partition in &partitions[..4] {
        builder.set(vnode_of(partition).to_index(), true);
    }
    let kept_vnodes = builder.finish();
    let expected: HashSet<_> = partitions
        .iter()
        .filter(|partition| kept_vnodes.is_set(vnode_of(partition).to_index()))
        .cloned()
        .collect();
    assert!(expected.len() < partitions.len());

    let store = MemoryStateStore::new();
    let (mut tx, mut executor) = create_executor_with_options(
        vec![session_count(5)],
        store,
        ExecutorOptions {
            vnodes: Some(Arc::new(Bitmap::ones(vnode_count))),
            ..ExecutorOptions::new(OptionalTables::WatermarkTrigger, LATEST)
        },
    )
    .await;

    tx.push_barrier(test_epoch(1), false);
    executor.expect_barrier().await;
    let rows = partitions
        .iter()
        .enumerate()
        .map(|(i, partition)| format!("+ 1 {partition} {i} 1"))
        .join("\n");
    tx.push_chunk(StreamChunk::from_pretty(&format!(" I T I i\n{rows}")));
    // Fill the trigger cache without closing any window.
    tx.push_watermark(0, DataType::Int64, ScalarImpl::Int64(2));
    tx.push_barrier(test_epoch(2), false);
    tx.send_barrier(
        Barrier::new_test_barrier(test_epoch(3)).with_mutation(Mutation::Update(UpdateMutation {
            vnode_bitmaps: [(ActorId::from(123), Arc::new(kept_vnodes))]
                .into_iter()
                .collect(),
            ..Default::default()
        })),
    );
    tx.push_watermark(0, DataType::Int64, ScalarImpl::Int64(10));
    tx.push_barrier(test_epoch(4), false);

    let mut closed = HashSet::new();
    let mut num_barriers = 0;
    while num_barriers < 3 {
        match executor.next().await.unwrap().unwrap() {
            Message::Chunk(chunk) => {
                for (_, row) in chunk.rows() {
                    closed.insert(row.datum_at(1).unwrap().into_utf8().to_owned());
                }
            }
            Message::Barrier(_) => num_barriers += 1,
            Message::Watermark(_) => {}
        }
    }
    assert_eq!(closed, expected);
}
