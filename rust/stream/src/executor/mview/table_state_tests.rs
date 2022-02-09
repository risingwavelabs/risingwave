use risingwave_common::array::Row;
use risingwave_common::util::sort_util::OrderType;
use risingwave_storage::bummock::BummockResult;
use risingwave_storage::memory::MemoryStateStore;
use risingwave_storage::table::mview::MViewTable;
use risingwave_storage::table::{ScannableTable, TableIter};
use risingwave_storage::Keyspace;

use crate::executor::test_utils::schemas;
use crate::executor::ManagedMViewState;

#[tokio::test]
async fn test_mview_table() {
    let state_store = MemoryStateStore::new();
    let schema = schemas::iii();
    let pk_columns = vec![0, 1];
    let orderings = vec![OrderType::Ascending, OrderType::Descending];
    let keyspace = Keyspace::executor_root(state_store, 0x42);
    let mut state = ManagedMViewState::new(
        keyspace.clone(),
        schema.clone(),
        pk_columns.clone(),
        orderings.clone(),
    );
    let table = MViewTable::new(keyspace.clone(), schema, pk_columns.clone(), orderings);
    let epoch: u64 = 0;

    state.put(
        Row(vec![Some(1_i32.into()), Some(11_i32.into())]),
        Row(vec![
            Some(1_i32.into()),
            Some(11_i32.into()),
            Some(111_i32.into()),
        ]),
    );
    state.put(
        Row(vec![Some(2_i32.into()), Some(22_i32.into())]),
        Row(vec![
            Some(2_i32.into()),
            Some(22_i32.into()),
            Some(222_i32.into()),
        ]),
    );
    state.delete(Row(vec![Some(2_i32.into()), Some(22_i32.into())]));
    state.flush(epoch).await.unwrap();

    let cell_1_0 = table
        .get(Row(vec![Some(1_i32.into()), Some(11_i32.into())]), 0)
        .await
        .unwrap();
    assert!(cell_1_0.is_some());
    assert_eq!(*cell_1_0.unwrap().unwrap().as_int32(), 1);
    let cell_1_1 = table
        .get(Row(vec![Some(1_i32.into()), Some(11_i32.into())]), 1)
        .await
        .unwrap();
    assert!(cell_1_1.is_some());
    assert_eq!(*cell_1_1.unwrap().unwrap().as_int32(), 11);
    let cell_1_2 = table
        .get(Row(vec![Some(1_i32.into()), Some(11_i32.into())]), 2)
        .await
        .unwrap();
    assert!(cell_1_2.is_some());
    assert_eq!(*cell_1_2.unwrap().unwrap().as_int32(), 111);

    let cell_2_0 = table
        .get(Row(vec![Some(2_i32.into()), Some(22_i32.into())]), 0)
        .await
        .unwrap();
    assert!(cell_2_0.is_none());
    let cell_2_1 = table
        .get(Row(vec![Some(2_i32.into()), Some(22_i32.into())]), 1)
        .await
        .unwrap();
    assert!(cell_2_1.is_none());
    let cell_2_2 = table
        .get(Row(vec![Some(2_i32.into()), Some(22_i32.into())]), 2)
        .await
        .unwrap();
    assert!(cell_2_2.is_none());
}

#[tokio::test]
async fn test_mview_table_for_string() {
    let state_store = MemoryStateStore::new();
    let schema = schemas::sss();
    let pk_columns = vec![0, 1];
    let orderings = vec![OrderType::Ascending, OrderType::Descending];
    let keyspace = Keyspace::executor_root(state_store, 0x42);

    let mut state = ManagedMViewState::new(
        keyspace.clone(),
        schema.clone(),
        pk_columns.clone(),
        orderings.clone(),
    );
    let table = MViewTable::new(keyspace.clone(), schema, pk_columns.clone(), orderings);
    let epoch: u64 = 0;

    state.put(
        Row(vec![
            Some("1".to_string().into()),
            Some("11".to_string().into()),
        ]),
        Row(vec![
            Some("1".to_string().into()),
            Some("11".to_string().into()),
            Some("111".to_string().into()),
        ]),
    );
    state.put(
        Row(vec![
            Some("2".to_string().into()),
            Some("22".to_string().into()),
        ]),
        Row(vec![
            Some("2".to_string().into()),
            Some("22".to_string().into()),
            Some("222".to_string().into()),
        ]),
    );
    state.delete(Row(vec![
        Some("2".to_string().into()),
        Some("22".to_string().into()),
    ]));
    state.flush(epoch).await.unwrap();

    let cell_1_0 = table
        .get(
            Row(vec![
                Some("1".to_string().into()),
                Some("11".to_string().into()),
            ]),
            0,
        )
        .await
        .unwrap();
    assert!(cell_1_0.is_some());
    assert_eq!(
        Some(cell_1_0.unwrap().unwrap().as_utf8().to_string()),
        Some("1".to_string())
    );
    let cell_1_1 = table
        .get(
            Row(vec![
                Some("1".to_string().into()),
                Some("11".to_string().into()),
            ]),
            1,
        )
        .await
        .unwrap();
    assert!(cell_1_1.is_some());
    assert_eq!(
        Some(cell_1_1.unwrap().unwrap().as_utf8().to_string()),
        Some("11".to_string())
    );
    let cell_1_2 = table
        .get(
            Row(vec![
                Some("1".to_string().into()),
                Some("11".to_string().into()),
            ]),
            2,
        )
        .await
        .unwrap();
    assert!(cell_1_2.is_some());
    assert_eq!(
        Some(cell_1_2.unwrap().unwrap().as_utf8().to_string()),
        Some("111".to_string())
    );

    let cell_2_0 = table
        .get(
            Row(vec![
                Some("2".to_string().into()),
                Some("22".to_string().into()),
            ]),
            0,
        )
        .await
        .unwrap();
    assert!(cell_2_0.is_none());
    let cell_2_1 = table
        .get(
            Row(vec![
                Some("2".to_string().into()),
                Some("22".to_string().into()),
            ]),
            1,
        )
        .await
        .unwrap();
    assert!(cell_2_1.is_none());
    let cell_2_2 = table
        .get(
            Row(vec![
                Some("2".to_string().into()),
                Some("22".to_string().into()),
            ]),
            2,
        )
        .await
        .unwrap();
    assert!(cell_2_2.is_none());
}

#[tokio::test]
async fn test_mview_table_iter() {
    let state_store = MemoryStateStore::new();
    let schema = schemas::iii();
    let pk_columns = vec![0, 1];
    let orderings = vec![OrderType::Ascending, OrderType::Descending];
    let keyspace = Keyspace::executor_root(state_store, 0x42);

    let mut state = ManagedMViewState::new(
        keyspace.clone(),
        schema.clone(),
        pk_columns.clone(),
        orderings.clone(),
    );
    let table = MViewTable::new(keyspace.clone(), schema, pk_columns.clone(), orderings);
    let epoch: u64 = 0;

    state.put(
        Row(vec![Some(1_i32.into()), Some(11_i32.into())]),
        Row(vec![
            Some(1_i32.into()),
            Some(11_i32.into()),
            Some(111_i32.into()),
        ]),
    );
    state.put(
        Row(vec![Some(2_i32.into()), Some(22_i32.into())]),
        Row(vec![
            Some(2_i32.into()),
            Some(22_i32.into()),
            Some(222_i32.into()),
        ]),
    );
    state.delete(Row(vec![Some(2_i32.into()), Some(22_i32.into())]));
    state.flush(epoch).await.unwrap();

    let mut iter = table.iter().await.unwrap();

    let res = iter.next().await.unwrap();
    assert!(res.is_some());
    assert_eq!(
        Row(vec![
            Some(1_i32.into()),
            Some(11_i32.into()),
            Some(111_i32.into())
        ]),
        res.unwrap()
    );

    let res = iter.next().await.unwrap();
    assert!(res.is_none());
}

#[tokio::test]
async fn test_multi_mview_table_iter() {
    let state_store = MemoryStateStore::new();
    let schema_1 = schemas::iii();
    let schema_2 = schemas::sss();
    let pk_columns = vec![0, 1];
    let orderings = vec![OrderType::Ascending, OrderType::Descending];

    let keyspace_1 = Keyspace::executor_root(state_store.clone(), 0x1111);
    let keyspace_2 = Keyspace::executor_root(state_store.clone(), 0x2222);
    let epoch: u64 = 0;

    let mut state_1 = ManagedMViewState::new(
        keyspace_1.clone(),
        schema_1.clone(),
        pk_columns.clone(),
        orderings.clone(),
    );
    let mut state_2 = ManagedMViewState::new(
        keyspace_2.clone(),
        schema_2.clone(),
        pk_columns.clone(),
        orderings.clone(),
    );

    let table_1 = MViewTable::new(
        keyspace_1.clone(),
        schema_1.clone(),
        pk_columns.clone(),
        orderings.clone(),
    );
    let table_2 = MViewTable::new(
        keyspace_2.clone(),
        schema_2.clone(),
        pk_columns.clone(),
        orderings,
    );

    state_1.put(
        Row(vec![Some(1_i32.into()), Some(11_i32.into())]),
        Row(vec![
            Some(1_i32.into()),
            Some(11_i32.into()),
            Some(111_i32.into()),
        ]),
    );
    state_1.put(
        Row(vec![Some(2_i32.into()), Some(22_i32.into())]),
        Row(vec![
            Some(2_i32.into()),
            Some(22_i32.into()),
            Some(222_i32.into()),
        ]),
    );
    state_1.delete(Row(vec![Some(2_i32.into()), Some(22_i32.into())]));

    state_2.put(
        Row(vec![
            Some("1".to_string().into()),
            Some("11".to_string().into()),
        ]),
        Row(vec![
            Some("1".to_string().into()),
            Some("11".to_string().into()),
            Some("111".to_string().into()),
        ]),
    );
    state_2.put(
        Row(vec![
            Some("2".to_string().into()),
            Some("22".to_string().into()),
        ]),
        Row(vec![
            Some("2".to_string().into()),
            Some("22".to_string().into()),
            Some("222".to_string().into()),
        ]),
    );
    state_2.delete(Row(vec![
        Some("2".to_string().into()),
        Some("22".to_string().into()),
    ]));

    state_1.flush(epoch).await.unwrap();
    state_2.flush(epoch).await.unwrap();

    let mut iter_1 = table_1.iter().await.unwrap();
    let mut iter_2 = table_2.iter().await.unwrap();

    let res_1_1 = iter_1.next().await.unwrap();
    assert!(res_1_1.is_some());
    assert_eq!(
        Row(vec![
            Some(1_i32.into()),
            Some(11_i32.into()),
            Some(111_i32.into()),
        ]),
        res_1_1.unwrap()
    );
    let res_1_2 = iter_1.next().await.unwrap();
    assert!(res_1_2.is_none());

    let res_2_1 = iter_2.next().await.unwrap();
    assert!(res_2_1.is_some());
    assert_eq!(
        Row(vec![
            Some("1".to_string().into()),
            Some("11".to_string().into()),
            Some("111".to_string().into())
        ]),
        res_2_1.unwrap()
    );
    let res_2_2 = iter_2.next().await.unwrap();
    assert!(res_2_2.is_none());
}

#[tokio::test]
async fn test_mview_scan_empty_column_ids_cardinality() {
    let state_store = MemoryStateStore::new();
    let schema = schemas::iii();
    let pk_columns = vec![0, 1];
    let orderings = vec![OrderType::Ascending, OrderType::Descending];
    let keyspace = Keyspace::executor_root(state_store, 0x42);

    let mut state = ManagedMViewState::new(
        keyspace.clone(),
        schema.clone(),
        pk_columns.clone(),
        orderings.clone(),
    );
    let table = MViewTable::new(keyspace.clone(), schema, pk_columns.clone(), orderings);
    let epoch: u64 = 0;

    state.put(
        Row(vec![Some(1_i32.into()), Some(11_i32.into())]),
        Row(vec![
            Some(1_i32.into()),
            Some(11_i32.into()),
            Some(111_i32.into()),
        ]),
    );
    state.put(
        Row(vec![Some(2_i32.into()), Some(22_i32.into())]),
        Row(vec![
            Some(2_i32.into()),
            Some(22_i32.into()),
            Some(222_i32.into()),
        ]),
    );
    state.flush(epoch).await.unwrap();

    let chunk = table.get_data_by_columns(&[]).await.unwrap();

    match chunk {
        BummockResult::Data(chunks) => {
            let chunk = chunks.into_iter().next().unwrap();
            assert_eq!(chunk.cardinality(), 2);
        }
        BummockResult::DataEof => panic!(),
    }
}
