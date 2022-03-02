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
            return Ok(None);
        }
        // We remark here that column_id may not be monotonically increasing, so `!=` instead of `<`
        // is needed here.
        let data_type = &self.table_column_descs[cell_id as usize].data_type;
        let datum = deserialize_cell(cell, data_type)?;
        assert!(self.data.get(cell_id as usize).unwrap().is_none());
        *self.data.get_mut(cell_id as usize).unwrap() = datum;
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

    #[test]
    fn test_cell_based_deserializer() {}
}
