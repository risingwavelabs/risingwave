use risingwave_common::catalog::Schema;

pub trait WithSchema {
    fn schema(&self) -> &Schema;
}
