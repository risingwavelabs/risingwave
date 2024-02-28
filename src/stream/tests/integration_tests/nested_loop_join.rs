use itertools::Itertools;
use risingwave_expr::expr::NonStrictExpression;
use risingwave_stream::executor::monitor::StreamingMetrics;
use risingwave_stream::executor::{
    JoinType, JoinTypePrimitive, NestedLoopJoinExecutor, StreamExecutorResult,
};

use crate::prelude::*;

async fn create_in_memory_state_table(
    mem_state: MemoryStateStore,
    data_types: &[DataType],
    order_types: &[OrderType],
    pk_indices: &[usize],
    table_id: u32,
) -> StateTable<MemoryStateStore> {
    let column_descs = data_types
        .iter()
        .enumerate()
        .map(|(id, data_type)| ColumnDesc::unnamed(ColumnId::new(id as i32), data_type.clone()))
        .collect_vec();
    StateTable::new_without_distribution(
        mem_state.clone(),
        TableId::new(table_id),
        column_descs,
        order_types.to_vec(),
        pk_indices.to_vec(),
    )
    .await
}

fn create_cond(condition_text: Option<String>) -> NonStrictExpression {
    build_from_pretty(
        condition_text
            .as_deref()
            .unwrap_or("(less_than:boolean $1:int8 $3:int8)"),
    )
}

async fn create_executor<const T: JoinTypePrimitive>(
    with_condition: bool,
    condition_text: Option<String>,
) -> (MessageSender, MessageSender, BoxedMessageStream) {
    let schema = Schema {
        fields: vec![
            Field::unnamed(DataType::Int64), // join key
            Field::unnamed(DataType::Int64),
        ],
    };
    let (tx_l, source_l) = MockSource::channel();
    let source_l = source_l.into_executor(schema.clone(), vec![1]);
    let (tx_r, source_r) = MockSource::channel();
    let source_r = source_r.into_executor(schema.clone(), vec![1]);
    let cond = with_condition.then(|| create_cond(condition_text));

    let mem_state = MemoryStateStore::new();

    let state_l = create_in_memory_state_table(
        mem_state.clone(),
        &[DataType::Int64, DataType::Int64],
        &[OrderType::ascending(), OrderType::ascending()],
        &[0, 1],
        0,
    )
    .await;

    let state_r = create_in_memory_state_table(
        mem_state,
        &[DataType::Int64, DataType::Int64],
        &[OrderType::ascending(), OrderType::ascending()],
        &[0, 1],
        2,
    )
    .await;

    let schema: Schema = [source_l.schema().fields(), source_r.schema().fields()]
        .concat()
        .into_iter()
        .collect();
    let schema_len = schema.len();

    let executor = NestedLoopJoinExecutor::<MemoryStateStore, T>::new(
        ActorContext::for_test(123),
        source_l,
        source_r,
        (0..schema_len).collect_vec(),
        cond,
        state_l,
        state_r,
        Arc::new(AtomicU64::new(0)),
        Arc::new(StreamingMetrics::unused()),
        1024,
    );
    (tx_l, tx_r, Box::new(executor).execute())
}

#[tokio::test]
async fn test_streaming_nested_loop_inner_join() -> StreamExecutorResult<()> {
    let chunk_l1 = StreamChunk::from_pretty(
        "  I I
         + 1 4
         + 2 5
         + 3 6",
    );
    let chunk_l2 = StreamChunk::from_pretty(
        "  I I
         + 3 8
         - 3 8",
    );
    let chunk_r1 = StreamChunk::from_pretty(
        "  I I
         + 2 7
         + 4 8
         + 6 9",
    );
    let chunk_r2 = StreamChunk::from_pretty(
        "  I  I
         + 3 2
         + 6 11",
    );
    let (mut tx_l, mut tx_r, mut hash_join) =
        create_executor::<{ JoinType::Inner }>(true, None).await;

    // push the init barrier for left and right
    tx_l.push_barrier(1, false);
    tx_r.push_barrier(1, false);
    hash_join.next_unwrap_ready_barrier()?;

    // push the 1st left chunk
    tx_l.push_chunk(chunk_l1);
    hash_join.next_unwrap_pending();

    // push the init barrier for left and right
    tx_l.push_barrier(2, false);
    tx_r.push_barrier(2, false);
    hash_join.next_unwrap_ready_barrier()?;

    // push the 2nd left chunk
    tx_l.push_chunk(chunk_l2);
    hash_join.next_unwrap_pending();

    // push the 1st right chunk
    tx_r.push_chunk(chunk_r1);
    // let chunk = hash_join.next_unwrap_ready_chunk()?;

    check_until_pending(
        &mut hash_join,
        expect_test::expect![[r#"
            - !chunk |-
              +---+---+---+---+---+
              | + | 1 | 4 | 2 | 7 |
              | + | 2 | 5 | 2 | 7 |
              | + | 3 | 6 | 2 | 7 |
              | + | 1 | 4 | 4 | 8 |
              | + | 2 | 5 | 4 | 8 |
              | + | 3 | 6 | 4 | 8 |
              | + | 1 | 4 | 6 | 9 |
              | + | 2 | 5 | 6 | 9 |
              | + | 3 | 6 | 6 | 9 |
              +---+---+---+---+---+
        "#]],
        SnapshotOptions::default(),
    );

    // push the 2nd right chunk
    tx_r.push_chunk(chunk_r2);
    // let chunk = hash_join.next_unwrap_ready_chunk()?;
    check_until_pending(
        &mut hash_join,
        expect_test::expect![[r#"
            - !chunk |-
              +---+---+---+---+----+
              | + | 1 | 4 | 6 | 11 |
              | + | 2 | 5 | 6 | 11 |
              | + | 3 | 6 | 6 | 11 |
              +---+---+---+---+----+
        "#]],
        SnapshotOptions::default(),
    );

    Ok(())
}
