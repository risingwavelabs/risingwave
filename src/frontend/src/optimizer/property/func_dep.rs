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

use std::collections::hash_map::Entry;
use std::collections::HashMap;

use fixedbitset::FixedBitSet;

use crate::utils::ColIndexMapping;
/// [`FunctionalDependencySet`] contains the functional dependencies.
///
/// It is used in optimizer to track the dependencies between columns.
#[derive(Debug, PartialEq, Clone, Default)]
pub struct FunctionalDependencySet {
    fd: HashMap<FixedBitSet, FixedBitSet>,
}

impl FunctionalDependencySet {
    /// Create a empty [`FunctionalDependencySet`]
    pub fn new() -> Self {
        Self { fd: HashMap::new() }
    }

    /// Create a [`FunctionalDependencySet`] with the indices of a key.
    ///
    /// # Examples
    /// ```rust
    /// # use risingwave_frontend::optimizer::property::FunctionalDependencySet;
    /// # use itertools::Itertools;
    /// let mut fd = FunctionalDependencySet::with_key(4, &[1]);
    /// let fd_inner = fd.into_dependencies();
    ///
    /// assert_eq!(fd_inner.len(), 1);
    ///
    /// let (from, to) = fd_inner.into_iter().next().unwrap();
    /// assert_eq!(from.ones().collect_vec(), &[1]);
    /// assert_eq!(to.ones().collect_vec(), &[0, 2, 3]);
    /// ```
    pub fn with_key(column_cnt: usize, key_indices: &[usize]) -> Self {
        let mut tmp = Self::new();
        tmp.add_key_column_by_indices(column_cnt, key_indices);
        tmp
    }

    /// Create a [`FunctionalDependencySet`] with a dependency [`HashMap`]
    pub fn with_dependencies(dependencies: HashMap<FixedBitSet, FixedBitSet>) -> Self {
        Self { fd: dependencies }
    }

    pub fn as_dependencies_mut(&mut self) -> &mut HashMap<FixedBitSet, FixedBitSet> {
        &mut self.fd
    }

    pub fn as_dependencies(&self) -> &HashMap<FixedBitSet, FixedBitSet> {
        &self.fd
    }

    pub fn into_dependencies(self) -> HashMap<FixedBitSet, FixedBitSet> {
        self.fd
    }

    /// Add a dependency to [`FunctionalDependencySet`] using [`FixedBitset`].
    ///
    /// # SAFETY
    /// The length of `from` and `to` must be the same.
    pub fn add_functional_dependency(&mut self, from: FixedBitSet, to: FixedBitSet) {
        assert_eq!(
            from.len(),
            to.len(),
            "from and to should have the same length"
        );
        match self.fd.entry(from) {
            Entry::Vacant(e) => {
                e.insert(to);
            }
            Entry::Occupied(mut e) => {
                e.get_mut().union_with(&to);
            }
        }
    }

    fn add_key_column_by_index(&mut self, column_cnt: usize, column_id: usize) {
        let mut from = FixedBitSet::with_capacity(column_cnt);
        from.set(column_id, true);
        let mut to = from.clone();
        to.toggle_range(0..to.len());
        self.add_functional_dependency(from, to);
    }

    /// Add key columns to a  [`FunctionalDependencySet`].
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use risingwave_frontend::optimizer::property::FunctionalDependencySet;
    /// # use itertools::Itertools;
    /// let mut fd = FunctionalDependencySet::new();
    /// fd.add_key_column_by_indices(4, &[1]);
    /// let fd_inner = fd.into_dependencies();
    ///
    /// assert_eq!(fd_inner.len(), 1);
    ///
    /// let (from, to) = fd_inner.into_iter().next().unwrap();
    /// assert_eq!(from.ones().collect_vec(), &[1]);
    /// assert_eq!(to.ones().collect_vec(), &[0, 2, 3]);
    /// ```
    pub fn add_key_column_by_indices(&mut self, column_cnt: usize, key_indices: &[usize]) {
        for &i in key_indices {
            self.add_key_column_by_index(column_cnt, i);
        }
    }

    /// Add constant columns to a  [`FunctionalDependencySet`].
    ///
    /// # Examples
    /// ```rust
    /// # use risingwave_frontend::optimizer::property::FunctionalDependencySet;
    /// # use itertools::Itertools;
    /// let mut fd = FunctionalDependencySet::new();
    /// fd.add_constant_column_by_index(4, 1);
    /// let fd_inner = fd.into_dependencies();
    ///
    /// assert_eq!(fd_inner.len(), 1);
    ///
    /// let (from, to) = fd_inner.into_iter().next().unwrap();
    /// assert!(from.ones().collect_vec().is_empty());
    /// assert_eq!(to.ones().collect_vec(), &[1]);
    /// ```
    pub fn add_constant_column_by_index(&mut self, column_cnt: usize, column_id: usize) {
        let mut to = FixedBitSet::with_capacity(column_cnt);
        to.set(column_id, true);
        self.add_functional_dependency(FixedBitSet::with_capacity(column_cnt), to);
    }

    /// Add a dependency to [`FunctionalDependencySet`] using column indices.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use risingwave_frontend::optimizer::property::FunctionalDependencySet;
    /// # use itertools::Itertools;
    /// let mut fd = FunctionalDependencySet::new();
    /// fd.add_functional_dependency_by_column_indices(&[1, 2], &[0], 4); // (1, 2) --> (0), 4 columns
    /// let fd_inner = fd.into_dependencies();
    ///
    /// assert_eq!(fd_inner.len(), 1);
    ///
    /// let (from, to) = fd_inner.into_iter().next().unwrap();
    /// assert_eq!(from.ones().collect_vec(), &[1, 2]);
    /// assert_eq!(to.ones().collect_vec(), &[0]);
    /// ```
    pub fn add_functional_dependency_by_column_indices(
        &mut self,
        from: &[usize],
        to: &[usize],
        column_cnt: usize,
    ) {
        let from = {
            let mut tmp = FixedBitSet::with_capacity(column_cnt);
            for &i in from {
                tmp.set(i, true);
            }
            tmp
        };
        let to = {
            let mut tmp = FixedBitSet::with_capacity(column_cnt);
            for &i in to {
                tmp.set(i, true);
            }
            tmp
        };
        self.add_functional_dependency(from, to)
    }

    fn get_closure(&self, columns: FixedBitSet) -> FixedBitSet {
        let mut closure = columns;
        let mut no_updates;
        loop {
            no_updates = true;
            for (from, to) in &self.fd {
                if from.is_subset(&closure) && !to.is_subset(&closure) {
                    closure.union_with(to);
                    no_updates = false;
                }
            }
            if no_updates {
                break;
            }
        }
        closure
    }

    /// Return `true` if the dependency determinant -> dependant exists.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use risingwave_frontend::optimizer::property::FunctionalDependencySet;
    /// # use fixedbitset::FixedBitSet;
    /// let mut fd = FunctionalDependencySet::new();
    /// fd.add_functional_dependency_by_column_indices(&[1, 2], &[0], 5); // (1, 2) --> (0)
    /// fd.add_functional_dependency_by_column_indices(&[0, 1], &[3], 5); // (0, 1) --> (3)
    /// fd.add_functional_dependency_by_column_indices(&[3], &[4], 5); // (3) --> (4)
    /// let from = FixedBitSet::from_iter([1usize, 2usize].into_iter());
    /// let to = FixedBitSet::from_iter([4usize].into_iter());
    /// assert!(fd.is_determined_by(from, to)); // (1, 2) --> (4)
    /// ```
    pub fn is_determined_by(&self, determinant: FixedBitSet, dependant: FixedBitSet) -> bool {
        self.get_closure(determinant).is_superset(&dependant)
    }

    pub fn rewrite_with_mapping(mut self, col_change: ColIndexMapping) -> Self {
        let mut new_fd = HashMap::new();
        for (from, to) in self.fd.drain() {
            assert_eq!(from.len(), col_change.source_size());
            assert_eq!(to.len(), col_change.source_size());
            let mut new_from = FixedBitSet::with_capacity(col_change.target_size());
            for i in from.ones() {
                if let Some(i) = col_change.try_map(i) {
                    new_from.insert(i);
                } else {
                    continue;
                }
            }
            let new_to = col_change.rewrite_bitset(&to);
            new_fd.insert(new_from, new_to);
        }
        Self { fd: new_fd }
    }
}
