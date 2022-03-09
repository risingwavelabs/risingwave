use fixedbitset::FixedBitSet;
use risingwave_common::catalog::Schema;

pub trait WithSchema {
    fn schema(&self) -> &Schema;

    fn must_contain_columns(&self, required_cols: &FixedBitSet) {
        assert!(
            required_cols.is_subset(&FixedBitSet::from_iter(0..self.schema().fields().len())),
            "Invalid required cols: {}, only {} columns available",
            required_cols,
            self.schema().fields().len()
        );
    }
}
