use std::cmp::max;
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
use std::fmt::Debug;
use std::vec;

use fixedbitset::FixedBitSet;
use itertools::Itertools;
use log::debug;

use crate::expr::{ExprImpl, ExprRewriter, InputRef};
use crate::optimizer::property::{Distribution, Order};

/// `ColIndexMapping` is a partial mapping from usize to usize.
///
/// It is used in optimizer for transformation of column index.
#[derive(Clone)]
pub struct ColIndexMapping {
    /// The size of the target space, i.e. target index is in the range `(0..target_size)`.
    target_size: usize,
    /// Each subscript is mapped to the corresponding element.
    map: Vec<Option<usize>>,
}

impl ColIndexMapping {
    /// Create a partial mapping which maps the subscripts range `(0..map.len())` to the
    /// corresponding element.
    pub fn new(map: Vec<Option<usize>>) -> Self {
        let target_size = match map.iter().filter_map(|x| *x).max_by_key(|x| *x) {
            Some(target_max) => target_max + 1,
            None => 0,
        };
        Self { map, target_size }
    }

    /// Create a partial mapping which maps from the subscripts range `(0..map.len())` to
    /// `(0..target_size)`. Each subscript is mapped to the corresponding element.
    pub fn with_target_size(map: Vec<Option<usize>>, target_size: usize) -> Self {
        if let Some(target_max) = map.iter().filter_map(|x| *x).max_by_key(|x| *x) {
            assert!(target_max < target_size)
        };
        Self { map, target_size }
    }
    pub fn into_parts(self) -> (Vec<Option<usize>>, usize) {
        (self.map, self.target_size)
    }

    pub fn identical_map(target_size: usize) -> Self {
        let map = (0..target_size).into_iter().map(Some).collect();
        Self::new(map)
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
        Self::with_target_size(map, following.target_size())
    }

    /// union two mapping, the result mapping target_size and source size will be the max size of
    /// the two mappings
    /// # Panics
    ///
    /// Will panic if a source appears in both to mapping
    #[must_use]
    pub fn union(&self, other: &Self) -> Self {
        debug!("union {:?} and {:?}", self, other);
        let target_size = max(self.target_size(), other.target_size());
        let source_size = max(self.source_size(), other.source_size());
        let mut map = vec![None; source_size];
        for (src, dst) in self.mapping_pairs() {
            assert_eq!(map[src], None);
            map[src] = Some(dst);
        }
        for (src, dst) in other.mapping_pairs() {
            assert_eq!(map[src], None);
            map[src] = Some(dst);
        }
        Self::with_target_size(map, target_size)
    }

    /// inverse the mapping, if a target corresponds more than one source, it will choose any one as
    /// it inverse mapping's target
    #[must_use]
    pub fn inverse(&self) -> Self {
        let mut map = vec![None; self.target_size()];
        for (src, dst) in self.mapping_pairs() {
            map[dst] = Some(src);
        }
        Self::with_target_size(map, self.source_size())
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

    /// Returns the size of the target range. Target index is in the range `(0..target_size)`.
    pub fn target_size(&self) -> usize {
        self.target_size
    }

    /// Returns the size of the source range. Source index is in the range `(0..source_size)`.
    pub fn source_size(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.target_size() == 0
    }

    pub fn rewrite_order(&self, mut order: Order) -> Order {
        for field in order.field_order.iter_mut() {
            field.index = self.map(field.index)
        }
        order
    }

    pub fn rewrite_distribution(&mut self, dist: Distribution) -> Distribution {
        match dist {
            Distribution::HashShard(mut col_idxes) => {
                for idx in col_idxes.iter_mut() {
                    *idx = self.map(*idx);
                }
                Distribution::HashShard(col_idxes)
            }
            _ => dist,
        }
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
            "ColIndexMapping(source_size:{}, target_size:{}, mapping:{})",
            self.source_size(),
            self.target_size(),
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
