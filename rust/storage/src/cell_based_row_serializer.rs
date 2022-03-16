use bytes::Bytes;
use risingwave_common::array::Row;
use risingwave_common::catalog::ColumnId;
use risingwave_common::error::Result;
use risingwave_common::util::ordered::serialize_pk_and_row;

#[derive(Clone)]
pub struct CellBasedRowSerializer {
    pk: Option<Vec<u8>>,
    row: Option<Row>,
    column_ids: Vec<ColumnId>,
}

impl CellBasedRowSerializer {
    pub fn new(column_ids: Vec<ColumnId>) -> Self {
        Self {
            pk: None,
            row: None,
            column_ids,
        }
    }

    /// When we encounter a new key, we can be sure that the previous row has been fully
    /// deserialized. Then we return the key and the value of the previous row.
    pub fn serialize(
        &mut self,
        pk: &Vec<u8>,
        row: Option<Row>,
        column_ids: Vec<ColumnId>,
    ) -> Result<Vec<(Vec<u8>, Option<Vec<u8>>)>> {
        serialize_pk_and_row(&pk, &row.clone(), &column_ids)
    }
}
