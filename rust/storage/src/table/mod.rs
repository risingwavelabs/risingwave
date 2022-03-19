pub mod cell_based_table;
// pub mod mview;

use risingwave_common::array::Row;
use risingwave_common::error::Result;

#[async_trait::async_trait]
pub trait TableIter: Send {
    async fn next(&mut self) -> Result<Option<Row>>;
}
