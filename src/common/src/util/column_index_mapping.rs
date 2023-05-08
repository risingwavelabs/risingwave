// Copyright 2023 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::cmp::max;
use std::fmt::Debug;
use std::vec;

use itertools::Itertools;
use risingwave_pb::catalog::PbColIndexMapping;
use risingwave_pb::stream_plan::DispatchStrategy;

/// `ColIndexMapping` is a partial mapping from usize to usize.
///
/// It is used in optimizer for transformation of column index.
#[derive(Clone, PartialEq, Eq, Hash)]
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
        Self { target_size, map }
    }

    /// Create a partial mapping which maps from the subscripts range `(0..map.len())` to
    /// `(0..target_size)`. Each subscript is mapped to the corresponding element.
    pub fn with_target_size(map: Vec<Option<usize>>, target_size: usize) -> Self {
        if let Some(target_max) = map.iter().filter_map(|x| *x).max_by_key(|x| *x) {
            assert!(target_max < target_size)
        };
        Self { target_size, map }
    }

    pub fn into_parts(self) -> (Vec<Option<usize>>, usize) {
        (self.map, self.target_size)
    }

    pub fn to_parts(&self) -> (&[Option<usize>], usize) {
        (&self.map, self.target_size)
    }

    pub fn put(&mut self, src: usize, tar: Option<usize>) {
        assert!(src < self.source_size());
        if let Some(tar) = tar {
            assert!(tar < self.target_size());
        }
        self.map[src] = tar;
    }

    pub fn identity(size: usize) -> Self {
        let map = (0..size).map(Some).collect();
        Self::new(map)
    }

    pub fn identity_or_none(source_size: usize, target_size: usize) -> Self {
        let map = (0..source_size)
            .map(|i| if i < target_size { Some(i) } else { None })
            .collect();
        Self::with_target_size(map, target_size)
    }

    pub fn empty(source_size: usize, target_size: usize) -> Self {
        let map = vec![None; source_size];
        Self::with_target_size(map, target_size)
    }

    /// Create a partial mapping which maps range `(0..source_num)` to range
    /// `(offset..offset+source_num)`.
    ///
    /// # Examples
    ///
    /// Positive offset:
    ///
    /// ```ignore
    /// # use risingwave_frontend::utils::ColIndexMapping;
    /// let mapping = ColIndexMapping::with_shift_offset(3, 3);
    /// assert_eq!(mapping.map(0), 3);
    /// assert_eq!(mapping.map(1), 4);
    /// assert_eq!(mapping.map(2), 5);
    /// ```
    ///
    /// Negative offset:
    ///
    ///  ```ignore
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
            .map(|source| {
                let target = source as isize + offset;
                usize::try_from(target).ok()
            })
            .collect_vec();
        let target_size = usize::try_from(source_num as isize + offset).unwrap();
        Self::with_target_size(map, target_size)
    }

    /// Maps the smallest index to 0, the next smallest to 1, and so on.
    ///
    /// It is useful for column pruning.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// # use fixedbitset::FixedBitSet;
    /// # use risingwave_frontend::utils::ColIndexMapping;
    /// let mut remaining_cols = vec![1, 3];
    /// let mapping = ColIndexMapping::with_remaining_columns(&remaining_cols, 4);
    /// assert_eq!(mapping.map(1), 0);
    /// assert_eq!(mapping.map(3), 1);
    /// assert_eq!(mapping.try_map(0), None);
    /// assert_eq!(mapping.try_map(2), None);
    /// assert_eq!(mapping.try_map(4), None);
    /// ```
    pub fn with_remaining_columns(cols: &[usize], src_size: usize) -> Self {
        let mut map = vec![None; src_size];
        for (tar, &src) in cols.iter().enumerate() {
            map[src] = Some(tar);
        }
        Self::new(map)
    }

    // TODO(yuchao): isn't this the same as `with_remaining_columns`?
    pub fn with_included_columns(cols: &[usize], src_size: usize) -> Self {
        let mut map = vec![None; src_size];
        for (tar, &src) in cols.iter().enumerate() {
            if map[src].is_none() {
                map[src] = Some(tar);
            }
        }
        Self::new(map)
    }

    /// Remove the given columns, and maps the remaining columns to a consecutive range starting
    /// from 0.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// # use fixedbitset::FixedBitSet;
    /// # use risingwave_frontend::utils::ColIndexMapping;
    /// let mut removed_cols = vec![0, 2, 4];
    /// let mapping = ColIndexMapping::with_removed_columns(&removed_cols, 5);
    /// assert_eq!(mapping.map(1), 0);
    /// assert_eq!(mapping.map(3), 1);
    /// assert_eq!(mapping.try_map(0), None);
    /// assert_eq!(mapping.try_map(2), None);
    /// assert_eq!(mapping.try_map(4), None);
    /// ```
    pub fn with_removed_columns(cols: &[usize], src_size: usize) -> Self {
        let cols = (0..src_size).filter(|x| !cols.contains(x)).collect_vec();
        Self::with_remaining_columns(&cols, src_size)
    }

    #[must_use]
    /// Compose column index mappings.
    /// For example if this maps 0->5,
    /// and `following` maps 5->1,
    /// Then the composite has 0->5->1 => 0->1.
    pub fn composite(&self, following: &Self) -> Self {
        // debug!("composing {:?} and {:?}", self, following);
        let mut map = self.map.clone();
        for target in &mut map {
            *target = target.and_then(|index| following.try_map(index));
        }
        Self::with_target_size(map, following.target_size())
    }

    pub fn clone_with_offset(&self, offset: usize) -> Self {
        let mut map = self.map.clone();
        for target in &mut map {
            *target = target.and_then(|index| index.checked_add(offset));
        }
        Self::with_target_size(map, self.target_size() + offset)
    }

    /// Union two mapping, the result mapping `target_size` and source size will be the max size
    /// of the two mappings.
    ///
    /// # Panics
    ///
    /// Will panic if a source appears in both to mapping
    #[must_use]
    pub fn union(&self, other: &Self) -> Self {
        // debug!("union {:?} and {:?}", self, other);
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

    /// Inverse the mapping. If a target corresponds to more than one source, return `None`.
    #[must_use]
    pub fn inverse(&self) -> Option<Self> {
        let mut map = vec![None; self.target_size()];
        for (src, dst) in self.mapping_pairs() {
            if map[dst].is_some() {
                return None;
            }
            map[dst] = Some(src);
        }
        Some(Self::with_target_size(map, self.source_size()))
    }

    /// return iter of (src, dst) order by src
    pub fn mapping_pairs(&self) -> impl Iterator<Item = (usize, usize)> + '_ {
        self.map
            .iter()
            .cloned()
            .enumerate()
            .filter_map(|(src, tar)| tar.map(|tar| (src, tar)))
    }

    /// Try mapping the source index to the target index.
    pub fn try_map(&self, index: usize) -> Option<usize> {
        *self.map.get(index)?
    }

    /// Try mapping all the source indices to the target indices. Returns `None` if any of the
    /// indices is not mapped.
    pub fn try_map_all(&self, indices: impl IntoIterator<Item = usize>) -> Option<Vec<usize>> {
        indices.into_iter().map(|i| self.try_map(i)).collect()
    }

    /// # Panics
    ///
    /// Will panic if `index >= self.source_size()` or `index` is not mapped.
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

    pub fn is_injective(&self) -> bool {
        let mut tar_exists = vec![false; self.target_size()];
        for i in self.map.iter().flatten() {
            if tar_exists[*i] {
                return false;
            }
            tar_exists[*i] = true;
        }
        true
    }
}

impl ColIndexMapping {
    pub fn to_protobuf(&self) -> PbColIndexMapping {
        PbColIndexMapping {
            target_size: self.target_size as u64,
            map: self
                .map
                .iter()
                .map(|x| x.map_or(-1, |x| x as i64))
                .collect(),
        }
    }

    pub fn from_protobuf(prost: &PbColIndexMapping) -> ColIndexMapping {
        ColIndexMapping {
            target_size: prost.target_size as usize,
            map: prost.map.iter().map(|&x| x.try_into().ok()).collect(),
        }
    }
}

impl ColIndexMapping {
    /// Rewrite the dist-key indices and output indices in the given dispatch strategy. Returns
    /// `None` if any of the indices is not mapped to the target.
    pub fn rewrite_dispatch_strategy(
        &self,
        strategy: &DispatchStrategy,
    ) -> Option<DispatchStrategy> {
        let map = |index: &[u32]| -> Option<Vec<u32>> {
            index
                .iter()
                .map(|i| self.try_map(*i as usize).map(|i| i as u32))
                .collect()
        };

        Some(DispatchStrategy {
            r#type: strategy.r#type,
            dist_key_indices: map(&strategy.dist_key_indices)?,
            output_indices: map(&strategy.output_indices)?,
        })
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
    use super::*;

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
    fn test_shift_0_source() {
        let mapping = ColIndexMapping::with_shift_offset(0, 3);
        assert_eq!(mapping.target_size(), 3);
    }

    #[test]
    fn test_composite() {
        let add_mapping = ColIndexMapping::with_shift_offset(3, 3);
        let remaining_cols = vec![3, 5];
        let col_prune_mapping = ColIndexMapping::with_remaining_columns(&remaining_cols, 6);
        let composite = add_mapping.composite(&col_prune_mapping);
        assert_eq!(composite.map(0), 0); // 0+3 = 3， 3 -> 0
        assert_eq!(composite.try_map(1), None);
        assert_eq!(composite.map(2), 1); // 2+3 = 5, 5 -> 1
    }
}
