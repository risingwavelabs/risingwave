#![allow(dead_code)]

use std::vec;

use fixedbitset::FixedBitSet;

/// `ColIndexMapping` is a mapping from usize to usize, and its source domain is [1..N]. it is used
/// in optimizer for transformation of column index. if the value in the vec is None, the source is
/// illegal.
pub struct ColIndexMapping {
    map: Vec<Option<usize>>,
}
impl ColIndexMapping {
    pub fn with_shift_offset(source_num: usize, offset: isize) -> Self {
        let map = (0..source_num)
            .into_iter()
            .map(|source| {
                let target = source as isize + offset;
                usize::try_from(target).ok()
            })
            .collect();
        Self { map }
    }

    pub fn with_remaining_columns(cols: &FixedBitSet) -> Self {
        let mut map = vec![None; cols.len()];
        for (tar, src) in cols.ones().enumerate() {
            map[src] = Some(tar);
        }
        Self { map }
    }

    pub fn with_removed_columns(cols: &FixedBitSet) -> Self {
        let mut cols = cols.clone();
        cols.toggle_range(..);
        Self::with_remaining_columns(&cols)
    }
    #[must_use]
    pub fn composite(&self, following: &Self) -> Self {
        let mut map = self.map.clone();
        for tar in &mut map {
            *tar = tar.and_then(|index| following.try_map(index));
        }
        Self { map }
    }

    pub fn try_map(&self, index: usize) -> Option<usize> {
        *self.map.get(index)?
    }

    pub fn map(&self, index: usize) -> usize {
        self.try_map(index).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use fixedbitset::FixedBitSet;

    use crate::utils::ColIndexMapping;

    #[test]
    fn test_add_mapping() {
        let mapping = ColIndexMapping::with_shift_offset(3, 0);
        assert_eq!(mapping.map(0), 0);
        assert_eq!(mapping.map(1), 1);
        assert_eq!(mapping.map(2), 2);
        let mapping = ColIndexMapping::with_shift_offset(3, 3);
        assert_eq!(mapping.map(0), 3);
        assert_eq!(mapping.map(1), 4);
        assert_eq!(mapping.map(2), 5);
    }

    #[test]
    fn test_minus_mapping() {
        let mapping = ColIndexMapping::with_shift_offset(3, 0);
        assert_eq!(mapping.map(0), 0);
        assert_eq!(mapping.map(1), 1);
        assert_eq!(mapping.map(2), 2);
        assert_eq!(mapping.try_map(3), None);
        assert_eq!(mapping.try_map(4), None);
        let mapping = ColIndexMapping::with_shift_offset(6, -3);
        assert_eq!(mapping.try_map(0), None);
        assert_eq!(mapping.try_map(1), None);
        assert_eq!(mapping.try_map(2), None);
        assert_eq!(mapping.map(3), 0);
        assert_eq!(mapping.map(4), 1);
        assert_eq!(mapping.map(5), 2);
        assert_eq!(mapping.try_map(6), None);
    }
    #[test]
    fn test_column_prune_mapping() {
        let mut remaining_cols = FixedBitSet::with_capacity(5);
        remaining_cols.insert(1);
        remaining_cols.insert(3);
        let mapping = ColIndexMapping::with_remaining_columns(&remaining_cols);
        assert_eq!(mapping.map(1), 0);
        assert_eq!(mapping.map(3), 1);
        let mut removed_cols = FixedBitSet::with_capacity(5);
        removed_cols.insert(0);
        removed_cols.insert(2);
        removed_cols.insert(4);
        let mapping = ColIndexMapping::with_removed_columns(&removed_cols);
        assert_eq!(mapping.map(1), 0);
        assert_eq!(mapping.map(3), 1);
    }
    #[test]
    fn test_column_prune_mapping_not_exist_1() {
        let mut remaining_cols = FixedBitSet::with_capacity(5);
        remaining_cols.insert(1);
        remaining_cols.insert(3);
        let mapping = ColIndexMapping::with_remaining_columns(&remaining_cols);
        assert_eq!(mapping.try_map(0), None);
    }
    #[test]
    fn test_column_prune_mapping_not_exist_2() {
        let mut remaining_cols = FixedBitSet::with_capacity(5);
        remaining_cols.insert(1);
        remaining_cols.insert(3);
        let mapping = ColIndexMapping::with_remaining_columns(&remaining_cols);
        assert_eq!(mapping.try_map(2), None);
    }
    #[test]
    fn test_column_prune_mapping_not_exist_3() {
        let mut remaining_cols = FixedBitSet::with_capacity(5);
        remaining_cols.insert(1);
        remaining_cols.insert(3);
        let mapping = ColIndexMapping::with_remaining_columns(&remaining_cols);
        assert_eq!(mapping.try_map(4), None);
    }
    #[test]
    fn test_column_prune_mapping_out_of_range() {
        let mut remaining_cols = FixedBitSet::with_capacity(5);
        remaining_cols.insert(1);
        remaining_cols.insert(3);
        let mapping = ColIndexMapping::with_remaining_columns(&remaining_cols);
        assert_eq!(mapping.try_map(5), None);
    }
    #[test]
    fn test_composite() {
        let add_mapping = ColIndexMapping::with_shift_offset(3, 3);
        let mut remaining_cols = FixedBitSet::with_capacity(6);
        remaining_cols.insert(3);
        remaining_cols.insert(5);
        let col_prune_mapping = ColIndexMapping::with_remaining_columns(&remaining_cols);
        let composite = add_mapping.composite(&col_prune_mapping);
        assert_eq!(composite.map(0), 0); // 0+3 = 3， 3 -> 0
        assert_eq!(composite.try_map(1), None);
        assert_eq!(composite.map(2), 1); // 2+3 = 5, 5 -> 1
    }
}
