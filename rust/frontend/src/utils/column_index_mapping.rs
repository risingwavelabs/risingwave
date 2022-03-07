#![allow(dead_code)]

use std::fmt::Debug;
use std::vec;

use fixedbitset::FixedBitSet;
use itertools::Itertools;
use log::debug;

use crate::expr::{ExprImpl, ExprRewriter, InputRef};

/// `ColIndexMapping` is a partial mapping from usize to usize.
///
/// It is used in optimizer for transformation of column index.
pub struct ColIndexMapping {
    target_upper: usize,
    /// The source column index is the subscript.
    map: Vec<Option<usize>>,
}

impl ColIndexMapping {
    /// Create a partial mapping which maps the subscripts range `(0..map.len())` to the
    /// corresponding element.
    pub fn new(map: Vec<Option<usize>>) -> Self {
        let target_upper = map
            .iter()
            .filter_map(|x| *x)
            .max_by_key(|x| *x)
            .unwrap_or(0);
        Self { map, target_upper }
    }

    /// Create a partial mapping which maps range `(0..source_num)` to range
    /// `(offset..offset+source_num)`.
    ///
    /// # Examples
    ///
    /// Positive offset:
    ///
    /// ```rust
    /// # use frontend::utils::ColIndexMapping;
    /// let mapping = ColIndexMapping::with_shift_offset(3, 3);
    /// assert_eq!(mapping.map(0), 3);
    /// assert_eq!(mapping.map(1), 4);
    /// assert_eq!(mapping.map(2), 5);
    /// ```
    ///
    /// Negative offset:
    ///
    ///  ```rust
    /// # use frontend::utils::ColIndexMapping;
    /// let mapping = ColIndexMapping::with_shift_offset(6, -3);
    /// assert_eq!(mapping.try_map(0), None);
    /// assert_eq!(mapping.try_map(1), None);
    /// assert_eq!(mapping.try_map(2), None);
    /// assert_eq!(mapping.map(3), 0);
    /// assert_eq!(mapping.map(4), 1);
    /// assert_eq!(mapping.map(5), 2);
    /// assert_eq!(mapping.try_map(6), None);
    /// ```
    pub fn with_shift_offset(source_num: usize, offset: isize) -> Self {
        let map = (0..source_num)
            .into_iter()
            .map(|source| {
                let target = source as isize + offset;
                usize::try_from(target).ok()
            })
            .collect_vec();
        Self::new(map)
    }

    /// Maps the smallest index to 0, the next smallest to 1, and so on.
    ///
    /// It is useful for column pruning.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use fixedbitset::FixedBitSet;
    /// # use frontend::utils::ColIndexMapping;
    /// let mut remaining_cols = FixedBitSet::with_capacity(5);
    /// remaining_cols.insert(1);
    /// remaining_cols.insert(3);
    /// let mapping = ColIndexMapping::with_remaining_columns(&remaining_cols);
    /// assert_eq!(mapping.map(1), 0);
    /// assert_eq!(mapping.map(3), 1);
    /// assert_eq!(mapping.try_map(0), None);
    /// assert_eq!(mapping.try_map(2), None);
    /// assert_eq!(mapping.try_map(4), None);
    /// ```
    pub fn with_remaining_columns(cols: &FixedBitSet) -> Self {
        let mut map = vec![None; cols.len()];
        for (tar, src) in cols.ones().enumerate() {
            map[src] = Some(tar);
        }
        Self::new(map)
    }

    /// Remove the given columns, and maps the remaining columns to a consecutive range starting
    /// from 0.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use fixedbitset::FixedBitSet;
    /// # use frontend::utils::ColIndexMapping;
    /// let mut removed_cols = FixedBitSet::with_capacity(5);
    /// removed_cols.insert(0);
    /// removed_cols.insert(2);
    /// removed_cols.insert(4);
    /// let mapping = ColIndexMapping::with_removed_columns(&removed_cols);
    /// assert_eq!(mapping.map(1), 0);
    /// assert_eq!(mapping.map(3), 1);
    /// assert_eq!(mapping.try_map(0), None);
    /// assert_eq!(mapping.try_map(2), None);
    /// assert_eq!(mapping.try_map(4), None);
    /// ```
    pub fn with_removed_columns(cols: &FixedBitSet) -> Self {
        let mut cols = cols.clone();
        cols.toggle_range(..);
        Self::with_remaining_columns(&cols)
    }

    #[must_use]
    pub fn composite(&self, following: &Self) -> Self {
        debug!("composing {:?} and {:?}", self, following);
        let mut map = self.map.clone();
        for tar in &mut map {
            *tar = tar.and_then(|index| following.try_map(index));
        }
        Self::new(map)
    }

    /// return iter of (src, dst) order by src
    pub fn mapping_pairs(&self) -> impl Iterator<Item = (usize, usize)> + '_ {
        self.map
            .iter()
            .cloned()
            .enumerate()
            .filter_map(|(src, tar)| tar.map(|tar| (src, tar)))
    }

    pub fn try_map(&self, index: usize) -> Option<usize> {
        *self.map.get(index)?
    }

    pub fn map(&self, index: usize) -> usize {
        self.try_map(index).unwrap()
    }

    pub fn target_upper(&self) -> usize {
        self.target_upper
    }

    pub fn source_upper(&self) -> usize {
        self.map.len() - 1
    }
}

impl ExprRewriter for ColIndexMapping {
    fn rewrite_input_ref(&mut self, input_ref: InputRef) -> ExprImpl {
        InputRef::new(self.map(input_ref.index()), input_ref.data_type()).into()
    }
}

impl Debug for ColIndexMapping {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ColIndexMapping({})",
            self.mapping_pairs()
                .map(|(src, dst)| format!("{}->{}", src, dst))
                .join(",")
        )
    }
}

#[cfg(test)]
mod tests {
    use fixedbitset::FixedBitSet;

    use crate::utils::ColIndexMapping;

    #[test]
    fn test_shift_0() {
        let mapping = ColIndexMapping::with_shift_offset(3, 0);
        assert_eq!(mapping.map(0), 0);
        assert_eq!(mapping.map(1), 1);
        assert_eq!(mapping.map(2), 2);
        assert_eq!(mapping.try_map(3), None);
        assert_eq!(mapping.try_map(4), None);
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
