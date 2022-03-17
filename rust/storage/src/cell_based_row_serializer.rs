use risingwave_common::array::Row;
use risingwave_common::catalog::ColumnId;
use risingwave_common::error::Result;
use risingwave_common::util::ordered::serialize_pk_and_row;

type KeyBytes = Vec<u8>;
type ValueBytes = Vec<u8>;
#[derive(Clone)]
pub struct CellBasedRowSerializer {}
impl Default for CellBasedRowSerializer {
    fn default() -> Self {
        Self::new()
    }
}
impl CellBasedRowSerializer {
    pub fn new() -> Self {
        Self {}
    }
    pub fn serialize(
        &mut self,
        pk: &[u8],
        row: Option<Row>,
        column_ids: Vec<ColumnId>,
    ) -> Result<Vec<(KeyBytes, Option<ValueBytes>)>> {
        serialize_pk_and_row(pk, &row, &column_ids)
    }
}
