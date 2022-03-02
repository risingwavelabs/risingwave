use bytes::Bytes;
use risingwave_common::array::Row;
use risingwave_common::error::Result;
use risingwave_common::types::Datum;
use risingwave_common::util::ordered::{
    deserialize_cell, deserialize_column_id, NULL_ROW_SPECIAL_CELL_ID,
};

use crate::TableColumnDesc;

#[derive(Clone)]
pub struct CellBasedRowDeserializer {
    table_column_descs: Vec<TableColumnDesc>,
    data: Vec<Datum>,
    /// `CellBasedRowDeserializer` does not deserialize pk itself. We need to take the key in as
    /// we have to know the cell id of each datum. So `pk_bytes` serves as an additional check
    /// which should also be done on the caller side.
    pk_bytes: Option<Vec<u8>>,
}

impl CellBasedRowDeserializer {
    pub fn new(table_column_descs: Vec<TableColumnDesc>) -> Self {
        let num_cells = table_column_descs.len();
        Self {
            table_column_descs,
            data: vec![None; num_cells],
            pk_bytes: None,
        }
    }

    /// When we encounter a new key, we can be sure that the previous row has been fully
    /// deserialized. Then we return the key and the value of the previous row.
    pub fn deserialize(
        &mut self,
        pk_with_cell_id: &Bytes,
        cell: &Bytes,
    ) -> Result<Option<(Vec<u8>, Row)>> {
        let pk_with_cell_id = pk_with_cell_id.to_vec();
        let pk_vec_len = pk_with_cell_id.len();
        let cur_pk_bytes = &pk_with_cell_id[0..pk_vec_len - 4];
        let mut result = None;
        if let Some(prev_pk_bytes) = &self.pk_bytes && prev_pk_bytes != cur_pk_bytes {
            result = self.take();
            self.pk_bytes = Some(cur_pk_bytes.to_vec());
        } else if self.pk_bytes.is_none() {
            self.pk_bytes = Some(cur_pk_bytes.to_vec());
        }

        let cell_id_bytes = &pk_with_cell_id[pk_vec_len - 4..];
        let cell_id = deserialize_column_id(cell_id_bytes)?;
        if cell_id == NULL_ROW_SPECIAL_CELL_ID {
            // do nothing
        } else {
            // We remark here that column_id may not be monotonically increasing, so `!=` instead of
            // `<` is needed here.
            let data_type = &self.table_column_descs[cell_id as usize].data_type;
            let datum = deserialize_cell(cell, data_type)?;
            assert!(self.data.get(cell_id as usize).unwrap().is_none());
            *self.data.get_mut(cell_id as usize).unwrap() = datum;
        }
        Ok(result)
    }

    pub fn take(&mut self) -> Option<(Vec<u8>, Row)> {
        if self.pk_bytes.is_none() {
            return None;
        }
        let cur_pk_bytes = self.pk_bytes.take().unwrap();
        let ret = self.data.iter_mut().map(std::mem::take).collect::<Vec<_>>();
        Some((cur_pk_bytes, Row(ret)))
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use risingwave_common::array::Row;
    use risingwave_common::catalog::ColumnId;
    use risingwave_common::types::{DataType, ScalarImpl};
    use risingwave_common::util::ordered::serialize_pk_and_row;

    use crate::cell_based_row_deserializer::CellBasedRowDeserializer;
    use crate::TableColumnDesc;

    #[test]
    fn test_cell_based_deserializer() {
        let column_ids = vec![ColumnId::from(0), ColumnId::from(1), ColumnId::from(2)];
        let table_column_descs = vec![
            TableColumnDesc::unnamed(column_ids[0].clone(), DataType::Char),
            TableColumnDesc::unnamed(column_ids[1].clone(), DataType::Int32),
            TableColumnDesc::unnamed(column_ids[2].clone(), DataType::Int64),
        ];
        let pk1 = vec![0u8, 0u8, 0u8, 0u8];
        let pk2 = vec![0u8, 0u8, 0u8, 1u8];
        let pk3 = vec![0u8, 0u8, 0u8, 2u8];
        let row1 = Row(vec![
            Some(ScalarImpl::Utf8("abc".to_string())),
            None,
            Some(ScalarImpl::Int64(1500)),
        ]);
        let row2 = Row(vec![None, None, None]);
        let row3 = Row(vec![
            None,
            Some(ScalarImpl::Int32(2020)),
            Some(ScalarImpl::Int64(2021)),
        ]);
        let bytes1 = serialize_pk_and_row(&pk1, &Some(row1.clone()), &column_ids).unwrap();
        let bytes2 = serialize_pk_and_row(&pk2, &Some(row2.clone()), &column_ids).unwrap();
        let bytes3 = serialize_pk_and_row(&pk3, &Some(row3.clone()), &column_ids).unwrap();
        let bytes = [bytes1, bytes2, bytes3].concat();

        let mut result = vec![];
        let mut deserializer = CellBasedRowDeserializer::new(table_column_descs);
        for (key_bytes, value_bytes) in bytes {
            let pk_and_row = deserializer
                .deserialize(&Bytes::from(key_bytes), &Bytes::from(value_bytes.unwrap()))
                .unwrap();
            if let Some(pk_and_row) = pk_and_row {
                result.push(pk_and_row.1);
            }
        }
        let pk_and_row = deserializer.take();
        result.push(pk_and_row.unwrap().1);
        assert_eq!(vec![row1, row2, row3], result);
    }
}
