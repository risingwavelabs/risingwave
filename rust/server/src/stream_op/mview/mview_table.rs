use bytes::Buf;
use risingwave_common::array::Row;
use risingwave_common::catalog::Schema;
use risingwave_common::error::{ErrorCode, Result};
use risingwave_common::types::{deserialize_datum_from, Datum};
use risingwave_common::util::sort_util::OrderType;

use super::*;
use crate::stream_op::keyspace::StateStore;
use crate::stream_op::StateStoreIter;

/// `MViewTable` provides a readable cell-based row table interface,
/// so that data can be queried by AP engine.
pub struct MViewTable<S: StateStore> {
    prefix: Vec<u8>,
    schema: Schema,
    pk_columns: Vec<usize>,
    sort_key_serializer: OrderedRowSerializer,
    storage: S,
}

impl<S: StateStore> MViewTable<S> {
    pub fn new(
        prefix: Vec<u8>,
        schema: Schema,
        pk_columns: Vec<usize>,
        orderings: Vec<OrderType>,
        storage: S,
    ) -> Self {
        let order_pairs = orderings
            .into_iter()
            .zip(pk_columns.clone().into_iter())
            .collect::<Vec<_>>();
        Self {
            prefix,
            schema,
            pk_columns,
            sort_key_serializer: OrderedRowSerializer::new(order_pairs),
            storage,
        }
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    // TODO(MrCroxx): remove me after iter is impled.
    pub fn storage(&self) -> S {
        self.storage.clone()
    }

    // TODO(MrCroxx): Refactor this after statestore iter is finished.
    pub fn iter(&self) -> MViewTableIter<S> {
        MViewTableIter::new(
            self.storage.iter(&self.prefix[..]),
            self.prefix.clone(),
            self.schema.clone(),
            self.pk_columns.clone(),
        )
    }

    // TODO(MrCroxx): More interfaces are needed besides cell get.
    pub async fn get(&self, pk: Row, cell_idx: usize) -> Result<Option<Datum>> {
        debug_assert!(cell_idx < self.schema.len());
        // TODO(MrCroxx): More efficient encoding is needed.
        let key = [
            &self.prefix[..],
            &serialize_pk(&pk, &self.sort_key_serializer)?[..],
            &serialize_cell_idx(cell_idx as u32)?[..],
        ]
        .concat();

        let buf = self
            .storage
            .get(&key)
            .await
            .map_err(|err| ErrorCode::InternalError(err.to_string()))?;
        match buf {
            Some(buf) => {
                let mut deserializer = memcomparable::Deserializer::new(buf);
                let datum = deserialize_datum_from(
                    &self.schema.fields[cell_idx].data_type.data_type_kind(),
                    &mut deserializer,
                )?;
                Ok(Some(datum))
            }
            None => Ok(None),
        }
    }
}

pub struct MViewTableIter<S: StateStore> {
    inner: S::Iter,
    prefix: Vec<u8>,
    schema: Schema,
    pk_columns: Vec<usize>,
}

impl<S: StateStore> MViewTableIter<S> {
    fn new(inner: S::Iter, prefix: Vec<u8>, schema: Schema, pk_columns: Vec<usize>) -> Self {
        Self {
            inner,
            prefix,
            schema,
            pk_columns,
        }
    }

    pub async fn open(&mut self) -> Result<()> {
        self.inner.open().await
    }

    pub async fn next(&mut self) -> Result<Option<Row>> {
        // TODO(MrCroxx): this implementation is inefficient, refactor me.
        let mut pk_buf = vec![];
        let mut columns = Row(vec![None; self.schema.len()]);
        let mut restored = 0;
        loop {
            match self.inner.next().await? {
                Some((key, value)) => {
                    // there is no need to deserialize pk in mview

                    if key.len() < self.prefix.len() + 4 {
                        return Err(ErrorCode::InternalError("corrupted key".to_owned()).into());
                    }

                    let cur_pk_buf = &key[self.prefix.len()..key.len() - 4];
                    if restored == 0 {
                        pk_buf = cur_pk_buf.to_owned();
                    } else if pk_buf != cur_pk_buf {
                        // previous item is incomplete
                        return Err(ErrorCode::InternalError("incomplete item".to_owned()).into());
                    }

                    // get cell_idx
                    let cell_idx = (&key[key.len() - 4..]).get_u32_le();

                    let mut cell_deserializer = memcomparable::Deserializer::new(value);
                    let cell = deserialize_datum_from(
                        &self.schema.fields[cell_idx as usize]
                            .data_type
                            .data_type_kind(),
                        &mut cell_deserializer,
                    )
                    .unwrap();

                    columns.0[cell_idx as usize] = cell;

                    restored += 1;
                    if restored == self.schema.len() {
                        return Ok(Some(columns));
                    }

                    // continue loop
                }
                // no more item
                None if restored == 0 => return Ok(None),
                // current item is incomplete
                None => return Err(ErrorCode::InternalError("incomplete item".to_owned()).into()),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::catalog::Field;
    use risingwave_common::types::{DataTypeKind, Int32Type, Scalar, StringType};
    use risingwave_common::util::sort_util::OrderType;

    use super::*;
    use crate::stream_op::MemoryStateStore;

    #[tokio::test]
    async fn test_mview_table() {
        let state_store = MemoryStateStore::default();
        let schema = Schema::new(vec![
            Field::new(Int32Type::create(false)),
            Field::new(Int32Type::create(false)),
            Field::new(Int32Type::create(false)),
        ]);
        let pk_columns = vec![0, 1];
        let orderings = vec![OrderType::Ascending, OrderType::Descending];
        let prefix = b"test-prefix-42".to_vec();
        let mut state = ManagedMViewState::new(
            prefix.clone(),
            schema.clone(),
            pk_columns.clone(),
            orderings.clone(),
            state_store.clone(),
        );
        let table = MViewTable::new(
            prefix.clone(),
            schema,
            pk_columns.clone(),
            orderings,
            state_store.clone(),
        );

        state.put(
            Row(vec![
                Some(1_i32.to_scalar_value()),
                Some(11_i32.to_scalar_value()),
            ]),
            Row(vec![
                Some(1_i32.to_scalar_value()),
                Some(11_i32.to_scalar_value()),
                Some(111_i32.to_scalar_value()),
            ]),
        );
        state.put(
            Row(vec![
                Some(2_i32.to_scalar_value()),
                Some(22_i32.to_scalar_value()),
            ]),
            Row(vec![
                Some(2_i32.to_scalar_value()),
                Some(22_i32.to_scalar_value()),
                Some(222_i32.to_scalar_value()),
            ]),
        );
        state.delete(Row(vec![
            Some(2_i32.to_scalar_value()),
            Some(22_i32.to_scalar_value()),
        ]));
        state.flush().await.unwrap();

        let cell_1_0 = table
            .get(
                Row(vec![
                    Some(1_i32.to_scalar_value()),
                    Some(11_i32.to_scalar_value()),
                ]),
                0,
            )
            .await
            .unwrap();
        assert!(cell_1_0.is_some());
        assert_eq!(*cell_1_0.unwrap().unwrap().as_int32(), 1);
        let cell_1_1 = table
            .get(
                Row(vec![
                    Some(1_i32.to_scalar_value()),
                    Some(11_i32.to_scalar_value()),
                ]),
                1,
            )
            .await
            .unwrap();
        assert!(cell_1_1.is_some());
        assert_eq!(*cell_1_1.unwrap().unwrap().as_int32(), 11);
        let cell_1_2 = table
            .get(
                Row(vec![
                    Some(1_i32.to_scalar_value()),
                    Some(11_i32.to_scalar_value()),
                ]),
                2,
            )
            .await
            .unwrap();
        assert!(cell_1_2.is_some());
        assert_eq!(*cell_1_2.unwrap().unwrap().as_int32(), 111);

        let cell_2_0 = table
            .get(
                Row(vec![
                    Some(2_i32.to_scalar_value()),
                    Some(22_i32.to_scalar_value()),
                ]),
                0,
            )
            .await
            .unwrap();
        assert!(cell_2_0.is_none());
        let cell_2_1 = table
            .get(
                Row(vec![
                    Some(2_i32.to_scalar_value()),
                    Some(22_i32.to_scalar_value()),
                ]),
                1,
            )
            .await
            .unwrap();
        assert!(cell_2_1.is_none());
        let cell_2_2 = table
            .get(
                Row(vec![
                    Some(2_i32.to_scalar_value()),
                    Some(22_i32.to_scalar_value()),
                ]),
                2,
            )
            .await
            .unwrap();
        assert!(cell_2_2.is_none());
    }

    #[tokio::test]
    async fn test_mview_table_for_string() {
        let state_store = MemoryStateStore::default();
        let schema = Schema::new(vec![
            Field::new(StringType::create(true, 0, DataTypeKind::Varchar)),
            Field::new(StringType::create(true, 0, DataTypeKind::Varchar)),
            Field::new(StringType::create(true, 0, DataTypeKind::Varchar)),
        ]);
        let pk_columns = vec![0, 1];
        let orderings = vec![OrderType::Ascending, OrderType::Descending];
        let prefix = b"test-prefix-42".to_vec();
        let mut state = ManagedMViewState::new(
            prefix.clone(),
            schema.clone(),
            pk_columns.clone(),
            orderings.clone(),
            state_store.clone(),
        );
        let table = MViewTable::new(
            prefix.clone(),
            schema,
            pk_columns.clone(),
            orderings,
            state_store.clone(),
        );

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
        state.flush().await.unwrap();

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
        let state_store = MemoryStateStore::default();
        let schema = Schema::new(vec![
            Field::new(Int32Type::create(false)),
            Field::new(Int32Type::create(false)),
            Field::new(Int32Type::create(false)),
        ]);
        let pk_columns = vec![0, 1];
        let orderings = vec![OrderType::Ascending, OrderType::Descending];
        let prefix = b"test-prefix-42".to_vec();
        let mut state = ManagedMViewState::new(
            prefix.clone(),
            schema.clone(),
            pk_columns.clone(),
            orderings.clone(),
            state_store.clone(),
        );
        let table = MViewTable::new(
            prefix.clone(),
            schema,
            pk_columns.clone(),
            orderings,
            state_store.clone(),
        );

        state.put(
            Row(vec![
                Some(1_i32.to_scalar_value()),
                Some(11_i32.to_scalar_value()),
            ]),
            Row(vec![
                Some(1_i32.to_scalar_value()),
                Some(11_i32.to_scalar_value()),
                Some(111_i32.to_scalar_value()),
            ]),
        );
        state.put(
            Row(vec![
                Some(2_i32.to_scalar_value()),
                Some(22_i32.to_scalar_value()),
            ]),
            Row(vec![
                Some(2_i32.to_scalar_value()),
                Some(22_i32.to_scalar_value()),
                Some(222_i32.to_scalar_value()),
            ]),
        );
        state.delete(Row(vec![
            Some(2_i32.to_scalar_value()),
            Some(22_i32.to_scalar_value()),
        ]));
        state.flush().await.unwrap();

        let mut iter = table.iter();
        iter.open().await.unwrap();

        let res = iter.next().await.unwrap();
        assert!(res.is_some());
        assert_eq!(
            Row(vec![
                Some(1_i32.to_scalar_value()),
                Some(11_i32.to_scalar_value()),
                Some(111_i32.to_scalar_value())
            ]),
            res.unwrap()
        );

        let res = iter.next().await.unwrap();
        assert!(res.is_none());
    }

    #[tokio::test]
    async fn test_multi_mview_table_iter() {
        let state_store = MemoryStateStore::default();
        let schema_1 = Schema::new(vec![
            Field::new(Int32Type::create(false)),
            Field::new(Int32Type::create(false)),
            Field::new(Int32Type::create(false)),
        ]);
        let schema_2 = Schema::new(vec![
            Field::new(StringType::create(true, 0, DataTypeKind::Varchar)),
            Field::new(StringType::create(true, 0, DataTypeKind::Varchar)),
            Field::new(StringType::create(true, 0, DataTypeKind::Varchar)),
        ]);
        let pk_columns = vec![0, 1];
        let orderings = vec![OrderType::Ascending, OrderType::Descending];
        let prefix_1 = b"test-prefix-1".to_vec();
        let prefix_2 = b"test-prefix-2".to_vec();

        let mut state_1 = ManagedMViewState::new(
            prefix_1.clone(),
            schema_1.clone(),
            pk_columns.clone(),
            orderings.clone(),
            state_store.clone(),
        );
        let mut state_2 = ManagedMViewState::new(
            prefix_2.clone(),
            schema_2.clone(),
            pk_columns.clone(),
            orderings.clone(),
            state_store.clone(),
        );

        let table_1 = MViewTable::new(
            prefix_1.clone(),
            schema_1.clone(),
            pk_columns.clone(),
            orderings.clone(),
            state_store.clone(),
        );
        let table_2 = MViewTable::new(
            prefix_2.clone(),
            schema_2.clone(),
            pk_columns.clone(),
            orderings,
            state_store.clone(),
        );

        state_1.put(
            Row(vec![
                Some(1_i32.to_scalar_value()),
                Some(11_i32.to_scalar_value()),
            ]),
            Row(vec![
                Some(1_i32.to_scalar_value()),
                Some(11_i32.to_scalar_value()),
                Some(111_i32.to_scalar_value()),
            ]),
        );
        state_1.put(
            Row(vec![
                Some(2_i32.to_scalar_value()),
                Some(22_i32.to_scalar_value()),
            ]),
            Row(vec![
                Some(2_i32.to_scalar_value()),
                Some(22_i32.to_scalar_value()),
                Some(222_i32.to_scalar_value()),
            ]),
        );
        state_1.delete(Row(vec![
            Some(2_i32.to_scalar_value()),
            Some(22_i32.to_scalar_value()),
        ]));

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

        state_1.flush().await.unwrap();
        state_2.flush().await.unwrap();

        let mut iter_1 = table_1.iter();
        let mut iter_2 = table_2.iter();
        iter_1.open().await.unwrap();
        iter_2.open().await.unwrap();

        let res_1_1 = iter_1.next().await.unwrap();
        assert!(res_1_1.is_some());
        assert_eq!(
            Row(vec![
                Some(1_i32.to_scalar_value()),
                Some(11_i32.to_scalar_value()),
                Some(111_i32.to_scalar_value()),
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
}
