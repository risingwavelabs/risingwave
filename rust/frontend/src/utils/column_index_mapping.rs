// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
use std::cmp::max;
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
    target_upper: Option<usize>,
    /// The source column index is the subscript.
    map: Vec<Option<usize>>,
}

impl ColIndexMapping {
    /// Create a partial mapping which maps the subscripts range `(0..map.len())` to the
    /// corresponding element.
    pub fn new(map: Vec<Option<usize>>) -> Self {
        let target_upper = map.iter().filter_map(|x| *x).max_by_key(|x| *x);
        Self { map, target_upper }
    }

    pub fn with_target_upper(map: Vec<Option<usize>>, target_upper: usize) -> Self {
        let max_target = map
            .iter()
            .filter_map(|x| *x)
            .max_by_key(|x| *x)
            .unwrap_or(0);
        assert!(max_target <= target_upper);
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
    /// # use risingwave_frontend::utils::ColIndexMapping;
    /// let mapping = ColIndexMapping::with_shift_offset(3, 3);
    /// assert_eq!(mapping.map(0), 3);
    /// assert_eq!(mapping.map(1), 4);
    /// assert_eq!(mapping.map(2), 5);
    /// ```
    ///
    /// Negative offset:
    ///
    ///  ```rust
    /// # use risingwave_frontend::utils::ColIndexMapping;
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
    /// # use risingwave_frontend::utils::ColIndexMapping;
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
    /// # use risingwave_frontend::utils::ColIndexMapping;
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
        Self::with_target_upper(map, max(self.target_upper(), following.target_upper()))
    }

    /// inverse the mapping, if a target corresponds more than one source, it will choose any one as
    /// it inverse mapping's target
    #[must_use]
    pub fn inverse(&self) -> Self {
        let mut map = vec![None; self.target_upper() + 1];
        for (src, dst) in self.mapping_pairs() {
            map[dst] = Some(src);
        }
        Self::with_target_upper(map, self.source_upper())
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

    /// Returns the maximum index in the target space.
    /// `None` means the mapping is empty.
    pub fn target_upper(&self) -> Option<usize> {
        self.target_upper
    }

    pub fn source_upper(&self) -> usize {
        self.map.len() - 1
    }

    pub fn is_empty(&self) -> bool {
        self.target_upper().is_none()
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
            "ColIndexMapping(source_upper:{}, target_upper:{}, mapping:{})",
            self.source_upper(),
            self.target_upper(),
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
