use risingwave_common::array::Row;
use risingwave_common::catalog::Schema;
use risingwave_common::error::{ErrorCode, Result};
use risingwave_common::types::{
    deserialize_datum_from, deserialize_datum_not_null_from, DataTypeKind, Datum,
};

use crate::stream_op::keyspace::StateStore;

use super::*;

use std::collections::btree_map::{BTreeMap, Entry};

/// `MViewTable` provides a readable cell-based row table interface,
/// so that data can be queried by AP engine.
pub struct MViewTable<S: StateStore> {
    prefix: Vec<u8>,
    schema: Schema,
    pk_columns: Vec<usize>,
    storage: S,
}

impl<S: StateStore> MViewTable<S> {
    pub fn new(prefix: Vec<u8>, schema: Schema, pk_columns: Vec<usize>, storage: S) -> Self {
        Self {
            prefix,
            schema,
            pk_columns,
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

    // TODO(MrCroxx): Refactor this after hummock iter is finished.
    pub async fn iter(&self) -> Result<<BTreeMap<Row, Row> as IntoIterator>::IntoIter> {
        let mut snapshot = self.storage.scan(&self.prefix[..], None).await?;
        let mut data = BTreeMap::new();
        for (key, value) in snapshot.drain(..) {
            let mut deserializer =
                memcomparable::Deserializer::from_slice(&key[self.prefix.len()..]);
            let pk = Row(self
                .pk_columns
                .iter()
                .map(|cell_idx| {
                    deserialize_datum_from(
                        &self.schema.fields[*cell_idx].data_type.data_type_kind(),
                        &mut deserializer,
                    )
                    .map_err(ErrorCode::MemComparableError)
                    .unwrap()
                })
                .collect());
            let cell_idx =
                *deserialize_datum_not_null_from(&DataTypeKind::Int32, &mut deserializer)
                    .map_err(ErrorCode::MemComparableError)
                    .unwrap()
                    .unwrap()
                    .as_int32();
            let mut deserializer = memcomparable::Deserializer::from_slice(&value);
            let cell = deserialize_datum_from(
                &self.schema.fields[cell_idx as usize]
                    .data_type
                    .data_type_kind(),
                &mut deserializer,
            )
            .map_err(ErrorCode::MemComparableError)
            .unwrap();
            match data.entry(pk) {
                Entry::Vacant(v) => {
                    let mut row = Row(vec![None; self.schema.len()]);
                    row.0[cell_idx as usize] = cell;
                    v.insert(row);
                }
                Entry::Occupied(mut o) => {
                    o.get_mut().0[cell_idx as usize] = cell;
                }
            }
        }
        Ok(data.into_iter())
    }

    // TODO(MrCroxx): More interfaces are needed besides cell get.
    pub async fn get(&self, pk: Row, cell_idx: usize) -> Result<Option<Datum>> {
        debug_assert!(cell_idx < self.schema.len());
        // TODO(MrCroxx): More efficient encoding is needed.
        let key = [
            &self.prefix[..],
            &serialize_pk(&pk)?[..],
            &serialize_cell_idx(cell_idx as i32)?[..],
        ]
        .concat();

        let buf = self
            .storage
            .get(&key)
            .await
            .map_err(|err| ErrorCode::InternalError(err.to_string()))?;
        match buf {
            Some(buf) => {
                let mut deserializer = memcomparable::Deserializer::from_slice(&buf);
                let datum = deserialize_datum_from(
                    &self.schema.fields[cell_idx].data_type.data_type_kind(),
                    &mut deserializer,
                )
                .map_err(ErrorCode::MemComparableError)?;
                Ok(Some(datum))
            }
            None => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::catalog::Field;
    use risingwave_common::types::{Int32Type, Scalar, StringType};

    use crate::stream_op::MemoryStateStore;

    use super::*;

    #[tokio::test]
    async fn test_mview_table() {
        // Only assert pk and columns can be successfully put/delete/flush,
        // and the ammount of rows is expected.
        let state_store = MemoryStateStore::default();
        let schema = Schema::new(vec![
            Field::new(Int32Type::create(false)),
            Field::new(Int32Type::create(false)),
        ]);
        let pk_columns = vec![0];
        let prefix = b"test-prefix-42".to_vec();
        let mut state = ManagedMViewState::new(
            prefix.clone(),
            schema.clone(),
            pk_columns.clone(),
            state_store.clone(),
        );
        let table = MViewTable::new(
            prefix.clone(),
            schema,
            pk_columns.clone(),
            state_store.clone(),
        );

        state.put(
            Row(vec![Some(1_i32.to_scalar_value())]),
            Row(vec![
                Some(1_i32.to_scalar_value()),
                Some(11_i32.to_scalar_value()),
            ]),
        );
        state.put(
            Row(vec![Some(2_i32.to_scalar_value())]),
            Row(vec![
                Some(2_i32.to_scalar_value()),
                Some(22_i32.to_scalar_value()),
            ]),
        );
        state.delete(Row(vec![Some(2_i32.to_scalar_value())]));
        state.flush().await.unwrap();

        let cell_1_0 = table
            .get(Row(vec![Some(1_i32.to_scalar_value())]), 0)
            .await
            .unwrap();
        assert!(cell_1_0.is_some());
        assert_eq!(*cell_1_0.unwrap().unwrap().as_int32(), 1);
        let cell_1_1 = table
            .get(Row(vec![Some(1_i32.to_scalar_value())]), 1)
            .await
            .unwrap();
        assert!(cell_1_1.is_some());
        assert_eq!(*cell_1_1.unwrap().unwrap().as_int32(), 11);

        let cell_2_0 = table
            .get(Row(vec![Some(2_i32.to_scalar_value())]), 0)
            .await
            .unwrap();
        assert!(cell_2_0.is_none());
        let cell_2_1 = table
            .get(Row(vec![Some(2_i32.to_scalar_value())]), 1)
            .await
            .unwrap();
        assert!(cell_2_1.is_none());
    }

    #[tokio::test]
    async fn test_mview_table_for_string() {
        // Only assert pk and columns can be successfully put/delete/flush,
        // and the ammount of rows is expected.
        let state_store = MemoryStateStore::default();
        let schema = Schema::new(vec![
            Field::new(StringType::create(true, 0, DataTypeKind::Varchar)),
            Field::new(StringType::create(true, 0, DataTypeKind::Varchar)),
        ]);
        let pk_columns = vec![0];
        let prefix = b"test-prefix-42".to_vec();
        let mut state = ManagedMViewState::new(
            prefix.clone(),
            schema.clone(),
            pk_columns.clone(),
            state_store.clone(),
        );
        let table = MViewTable::new(
            prefix.clone(),
            schema,
            pk_columns.clone(),
            state_store.clone(),
        );

        state.put(
            Row(vec![Some("1".to_string().into())]),
            Row(vec![
                Some("1".to_string().into()),
                Some("1.1".to_string().into()),
            ]),
        );
        state.put(
            Row(vec![Some("2".to_string().into())]),
            Row(vec![
                Some("2".to_string().into()),
                Some("2.2".to_string().into()),
            ]),
        );
        state.delete(Row(vec![Some("2".to_string().into())]));
        state.flush().await.unwrap();

        let cell_1_0 = table
            .get(Row(vec![Some("1".to_string().into())]), 0)
            .await
            .unwrap();
        assert!(cell_1_0.is_some());
        assert_eq!(
            Some(cell_1_0.unwrap().unwrap().as_utf8().to_string()),
            Some("1".to_string())
        );
        let cell_1_1 = table
            .get(Row(vec![Some("1".to_string().into())]), 1)
            .await
            .unwrap();
        assert!(cell_1_1.is_some());
        assert_eq!(
            Some(cell_1_1.unwrap().unwrap().as_utf8().to_string()),
            Some("1.1".to_string())
        );

        let cell_2_0 = table
            .get(Row(vec![Some("2".to_string().into())]), 0)
            .await
            .unwrap();
        assert!(cell_2_0.is_none());
        let cell_2_1 = table
            .get(Row(vec![Some("2".to_string().into())]), 1)
            .await
            .unwrap();
        assert!(cell_2_1.is_none());
    }
}
