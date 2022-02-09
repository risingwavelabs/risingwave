#![allow(dead_code)]

use std::vec;

use fixedbitset::FixedBitSet;
pub trait ColIndexMapping {
    /// panic if the input index is out of source
    fn map(&self, index: usize) -> usize;
}
pub type BoxedColIndexMapping = Box<dyn ColIndexMapping>;

struct CompositeMapping {
    mapping1: BoxedColIndexMapping,
    mapping2: BoxedColIndexMapping,
}
impl CompositeMapping {
    fn new(mapping1: BoxedColIndexMapping, mapping2: BoxedColIndexMapping) -> Self {
        Self { mapping1, mapping2 }
    }
}
impl ColIndexMapping for CompositeMapping {
    fn map(&self, index: usize) -> usize {
        self.mapping2.map(self.mapping1.map(index))
    }
}

struct ColPruneMapping {
    map: Vec<Option<usize>>,
}
impl ColIndexMapping for ColPruneMapping {
    fn map(&self, index: usize) -> usize {
        self.map[index].unwrap()
    }
}
impl ColPruneMapping {
    fn with_remaining_columns(cols: &FixedBitSet) -> Self {
        let mut map = vec![None; cols.len()];
        for (tar, src) in cols.ones().enumerate() {
            map[src] = Some(tar);
        }
        ColPruneMapping { map }
    }
    fn with_removed_columns(cols: &FixedBitSet) -> Self {
        let mut cols = cols.clone();
        cols.toggle_range(..);
        Self::with_remaining_columns(&cols)
    }
}

struct AddMapping {
    delta: usize,
}

impl AddMapping {
    pub fn new(delta: usize) -> Self {
        Self { delta }
    }
}
impl ColIndexMapping for AddMapping {
    fn map(&self, index: usize) -> usize {
        index + self.delta
    }
}

struct MinusMapping {
    delta: usize,
}

impl ColIndexMapping for MinusMapping {
    fn map(&self, index: usize) -> usize {
        assert!(index >= self.delta);
        index - self.delta
    }
}

impl MinusMapping {
    pub fn new(delta: usize) -> Self {
        Self { delta }
    }
}
#[cfg(test)]
mod tests {
    use fixedbitset::FixedBitSet;

    use super::{AddMapping, CompositeMapping};
    use crate::utils::column_index_mapping::{ColPruneMapping, MinusMapping};
    use crate::utils::ColIndexMapping;

    #[test]
    fn test_add_mapping() {
        let mapping = AddMapping::new(0);
        assert_eq!(mapping.map(0), 0);
        assert_eq!(mapping.map(1), 1);
        assert_eq!(mapping.map(2), 2);
        let mapping = AddMapping::new(3);
        assert_eq!(mapping.map(0), 3);
        assert_eq!(mapping.map(1), 4);
        assert_eq!(mapping.map(2), 5);
    }

    #[test]
    fn test_minus_mapping() {
        let mapping = MinusMapping::new(0);
        assert_eq!(mapping.map(0), 0);
        assert_eq!(mapping.map(1), 1);
        assert_eq!(mapping.map(2), 2);
        let mapping = MinusMapping::new(3);
        assert_eq!(mapping.map(3), 0);
        assert_eq!(mapping.map(4), 1);
        assert_eq!(mapping.map(5), 2);
    }
    #[test]
    #[should_panic]
    fn test_minus_mapping_panic() {
        let mapping = MinusMapping::new(3);
        mapping.map(2);
    }
    #[test]
    fn test_column_prune_mapping() {
        let mut remaining_cols = FixedBitSet::with_capacity(5);
        remaining_cols.insert(1);
        remaining_cols.insert(3);
        let mapping = ColPruneMapping::with_remaining_columns(&remaining_cols);
        assert_eq!(mapping.map(1), 0);
        assert_eq!(mapping.map(3), 1);
        let mut removed_cols = FixedBitSet::with_capacity(5);
        removed_cols.insert(0);
        removed_cols.insert(2);
        removed_cols.insert(4);
        let mapping = ColPruneMapping::with_removed_columns(&removed_cols);
        assert_eq!(mapping.map(1), 0);
        assert_eq!(mapping.map(3), 1);
    }
    #[test]
    #[should_panic]
    fn test_column_prune_mapping_not_exist_1() {
        let mut remaining_cols = FixedBitSet::with_capacity(5);
        remaining_cols.insert(1);
        remaining_cols.insert(3);
        let mapping = ColPruneMapping::with_remaining_columns(&remaining_cols);
        mapping.map(0);
    }
    #[test]
    #[should_panic]
    fn test_column_prune_mapping_not_exist_2() {
        let mut remaining_cols = FixedBitSet::with_capacity(5);
        remaining_cols.insert(1);
        remaining_cols.insert(3);
        let mapping = ColPruneMapping::with_remaining_columns(&remaining_cols);
        mapping.map(2);
    }
    #[test]
    #[should_panic]
    fn test_column_prune_mapping_not_exist_3() {
        let mut remaining_cols = FixedBitSet::with_capacity(5);
        remaining_cols.insert(1);
        remaining_cols.insert(3);
        let mapping = ColPruneMapping::with_remaining_columns(&remaining_cols);
        mapping.map(4);
    }
    #[test]
    #[should_panic]
    fn test_column_prune_mapping_out_of_range() {
        let mut remaining_cols = FixedBitSet::with_capacity(5);
        remaining_cols.insert(1);
        remaining_cols.insert(3);
        let mapping = ColPruneMapping::with_remaining_columns(&remaining_cols);
        mapping.map(5);
    }
    #[test]
    fn test_composite_mapping() {
        let add_mapping = AddMapping::new(3);
        let mut remaining_cols = FixedBitSet::with_capacity(6);
        remaining_cols.insert(3);
        remaining_cols.insert(5);
        let col_prune_mapping = ColPruneMapping::with_remaining_columns(&remaining_cols);
        let composite_mapping =
            CompositeMapping::new(Box::new(add_mapping), Box::new(col_prune_mapping));
        assert_eq!(composite_mapping.map(0), 0); // 0+3 = 3， 3-> 0
        assert_eq!(composite_mapping.map(2), 1); // 2+3 = 5, 5 -> 1
    }
}
