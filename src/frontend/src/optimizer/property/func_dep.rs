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

use std::fmt;

use fixedbitset::FixedBitSet;
use itertools::Itertools;

/// [`FunctionalDependency`] represent a dependency of from --> to.
///
/// For columns ABCD, the FD AC --> B is represented as {0, 2} --> {1} using `FixedBitset`.
#[derive(Debug, PartialEq, Eq, Clone, Default, Hash)]
pub struct FunctionalDependency {
    from: FixedBitSet,
    to: FixedBitSet,
}

impl fmt::Display for FunctionalDependency {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let from = self.from.ones().collect_vec();
        let to = self.to.ones().collect_vec();
        f.write_fmt(format_args!("{:?} --> {:?}", from, to))
    }
}

impl FunctionalDependency {
    /// Create a [`FunctionalDependency`] with bitset.
    /// This indicate a from --> to dependency.
    pub fn new(from: FixedBitSet, to: FixedBitSet) -> Self {
        assert_eq!(
            from.len(),
            to.len(),
            "from and to should have the same length"
        );
        assert!(!to.is_clear(), "`to` should contains at least one element");
        FunctionalDependency { from, to }
    }

    pub fn from(&self) -> &FixedBitSet {
        &self.from
    }

    pub fn to(&self) -> &FixedBitSet {
        &self.to
    }

    /// Grow the capacity of [`FunctionalDependency`] to **columns**, all new columns initialized to
    /// zero.
    pub fn grow(&mut self, columns: usize) {
        self.from.grow(columns);
        self.to.grow(columns);
    }

    pub fn set_from(&mut self, column_index: usize, enabled: bool) {
        self.from.set(column_index, enabled);
    }

    pub fn set_to(&mut self, column_index: usize, enabled: bool) {
        self.to.set(column_index, enabled);
    }

    /// Create a [`FunctionalDependency`] with column indices. The order of the indices doesn't
    /// matter. It is treated as a combination of columns.
    pub fn with_indices(column_cnt: usize, from: &[usize], to: &[usize]) -> Self {
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
        FunctionalDependency { from, to }
    }

    /// Create a [`FunctionalDependency`] with a key. The combination of these columns can determine
    /// all other columns.
    fn with_key(column_cnt: usize, key_indices: &[usize]) -> Self {
        let mut from = FixedBitSet::with_capacity(column_cnt);
        for &idx in key_indices {
            from.set(idx, true);
        }
        let mut to = from.clone();
        to.toggle_range(0..to.len());
        FunctionalDependency { from, to }
    }

    /// Create a [`FunctionalDependency`] for constant columns.
    /// These columns can be determined by any column.
    pub fn with_constant(column_cnt: usize, constant_indices: &[usize]) -> Self {
        let mut to = FixedBitSet::with_capacity(column_cnt);
        for &i in constant_indices {
            to.set(i, true);
        }
        FunctionalDependency {
            from: FixedBitSet::with_capacity(column_cnt),
            to,
        }
    }

    pub fn into_parts(self) -> (FixedBitSet, FixedBitSet) {
        (self.from, self.to)
    }
}

/// [`FunctionalDependencySet`] contains the functional dependencies.
///
/// It is used in optimizer to track the dependencies between columns.
#[derive(Debug, PartialEq, Clone, Default)]
pub struct FunctionalDependencySet {
    /// the number of columns
    column_count: usize,
    /// `strict` contains all strict functional dependencies.
    ///
    /// The strict functional dependency use the **NULL=** semantic. It means that all NULLs are
    /// considered as equal. So for following table, A --> B is not valid.
    /// **NOT** allowed.
    /// ```text
    ///   A   | B
    /// ------|---
    ///  NULL | 1
    ///  NULL | 2
    /// ```
    ///
    /// Currently we only have strict dependencies, but we may also have lax dependencies in the
    /// future.
    strict: Vec<FunctionalDependency>,
}

impl fmt::Display for FunctionalDependencySet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("{")?;
        self.strict
            .iter()
            .format_with(", ", |fd, f| f(&format_args!("{}", fd)))
            .fmt(f)?;
        f.write_str("}")
    }
}

impl FunctionalDependencySet {
    /// Create an empty [`FunctionalDependencySet`]
    pub fn new(column_count: usize) -> Self {
        Self {
            strict: Vec::new(),
            column_count,
        }
    }

    /// Create a [`FunctionalDependencySet`] with the indices of a key.
    ///
    /// The **combination** of these columns can determine all other columns.
    pub fn with_key(column_cnt: usize, key_indices: &[usize]) -> Self {
        let mut tmp = Self::new(column_cnt);
        tmp.add_key(key_indices);
        tmp
    }

    /// Create a [`FunctionalDependencySet`] with a dependency [`Vec`]
    pub fn with_dependencies(
        column_count: usize,
        strict_dependencies: Vec<FunctionalDependency>,
    ) -> Self {
        for i in &strict_dependencies {
            assert_eq!(column_count, i.from().len())
        }
        Self {
            strict: strict_dependencies,
            column_count,
        }
    }

    pub fn as_dependencies_mut(&mut self) -> &mut Vec<FunctionalDependency> {
        &mut self.strict
    }

    pub fn as_dependencies(&self) -> &Vec<FunctionalDependency> {
        &self.strict
    }

    pub fn into_dependencies(self) -> Vec<FunctionalDependency> {
        self.strict
    }

    /// Add a functional dependency to a [`FunctionalDependencySet`].
    pub fn add_functional_dependency(&mut self, fd: FunctionalDependency) {
        let FunctionalDependency { from, to } = fd;
        assert_eq!(self.column_count, from.len());
        if !to.is_clear() {
            match self.strict.iter().position(|elem| elem.from == from) {
                Some(idx) => self.strict[idx].to.union_with(&to),
                None => self.strict.push(FunctionalDependency::new(from, to)),
            }
        }
    }

    /// Add key columns to a  [`FunctionalDependencySet`].
    pub fn add_key(&mut self, key_indices: &[usize]) {
        self.add_functional_dependency(FunctionalDependency::with_key(
            self.column_count,
            key_indices,
        ));
    }

    /// Add constant columns to a  [`FunctionalDependencySet`].
    pub fn add_constant_columns(&mut self, constant_columns: &[usize]) {
        self.add_functional_dependency(FunctionalDependency::with_constant(
            self.column_count,
            constant_columns,
        ));
    }

    /// Add a dependency to [`FunctionalDependencySet`] using column indices.
    pub fn add_functional_dependency_by_column_indices(&mut self, from: &[usize], to: &[usize]) {
        self.add_functional_dependency(FunctionalDependency::with_indices(
            self.column_count,
            from,
            to,
        ))
    }

    fn get_closure(&self, columns: &FixedBitSet) -> FixedBitSet {
        let mut closure = columns.clone();
        let mut no_updates;
        loop {
            no_updates = true;
            for FunctionalDependency { from, to } in &self.strict {
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
    pub fn is_determined_by(&self, determinant: &FixedBitSet, dependant: &FixedBitSet) -> bool {
        self.get_closure(determinant).is_superset(dependant)
    }

    fn is_key_inner(&self, columns: &FixedBitSet) -> bool {
        let all_columns = {
            let mut tmp = columns.clone();
            tmp.set_range(.., true);
            tmp
        };
        self.is_determined_by(columns, &all_columns)
    }

    /// Return true if the combination of `columns` can fully determine other columns.
    pub fn is_key(&self, columns: &[usize]) -> bool {
        let mut key_bitset = FixedBitSet::from_iter(columns.iter().copied());
        key_bitset.grow(self.column_count);
        self.is_key_inner(&key_bitset)
    }

    /// Remove redundant columns from the given set.
    ///
    /// Redundant columns can be functionally determined by other columns so there is no need to
    /// keep them in a key.
    ///
    /// Note that the accurate minimization algorithm can take O(2^n) time,
    /// so we use a approximate algorithm which use O(cn) time. `c` is the number of columns, and
    /// `n` is the number of functional dependency rules.
    ///
    /// This algorithm may not necessarily find the key with the least number of columns.
    /// But it will ensure that no redundant columns will be preserved.
    pub fn minimize_key(&self, key_indices: &[usize]) -> Vec<usize> {
        let mut key_bitset = FixedBitSet::from_iter(key_indices.iter().copied());
        key_bitset.grow(self.column_count);
        let res = self.minimize_key_inner(key_bitset);
        res.ones().collect_vec()
    }

    /// This algorithm removes columns one by one and check
    /// whether the remaining columns can form a key or not. If the remaining columns can form a
    /// key, then this column can be removed.
    fn minimize_key_inner(&self, key: FixedBitSet) -> FixedBitSet {
        assert!(
            self.is_key_inner(&key),
            "{:?} is not a key!",
            key.ones().into_iter().collect_vec()
        );
        let mut new_key = key.clone();
        for i in key.ones() {
            new_key.set(i, false);
            if !self.is_key_inner(&new_key) {
                new_key.set(i, true);
            }
        }
        new_key
    }
}

#[cfg(test)]
mod tests {
    use fixedbitset::FixedBitSet;
    use itertools::Itertools;

    use super::FunctionalDependencySet;

    #[test]
    fn test_minimize_key() {
        let mut fd_set = FunctionalDependencySet::new(5);
        // [0, 2, 3, 4] is a key
        fd_set.add_key(&[0, 2, 3, 4]);
        // 0 is constant
        fd_set.add_constant_columns(&[0]);
        // 1, 2 --> 3
        fd_set.add_functional_dependency_by_column_indices(&[1, 2], &[3]);
        // 0, 2 --> 3
        fd_set.add_functional_dependency_by_column_indices(&[0, 2], &[3]);
        // 3, 4 --> 2
        fd_set.add_functional_dependency_by_column_indices(&[3, 4], &[2]);
        // therefore, column 0 and column 2 can be removed from key
        let key = fd_set.minimize_key(&[0, 2, 3, 4]);
        assert_eq!(key, &[3, 4]);
    }

    #[test]
    fn test_with_key() {
        let fd = FunctionalDependencySet::with_key(4, &[1]);
        let fd_inner = fd.into_dependencies();

        assert_eq!(fd_inner.len(), 1);

        let (from, to) = fd_inner[0].clone().into_parts();
        // 1 --> 0, 2, 3
        assert_eq!(from.ones().collect_vec(), &[1]);
        assert_eq!(to.ones().collect_vec(), &[0, 2, 3]);
    }

    #[test]
    fn test_add_key() {
        let mut fd = FunctionalDependencySet::new(4);
        fd.add_key(&[1]);
        let fd_inner = fd.into_dependencies();

        assert_eq!(fd_inner.len(), 1);

        let (from, to) = fd_inner[0].clone().into_parts();
        assert_eq!(from.ones().collect_vec(), &[1]);
        assert_eq!(to.ones().collect_vec(), &[0, 2, 3]);
    }

    #[test]
    fn test_add_constant_columns() {
        let mut fd = FunctionalDependencySet::new(4);
        fd.add_constant_columns(&[1]);
        let fd_inner = fd.into_dependencies();

        assert_eq!(fd_inner.len(), 1);

        let (from, to) = fd_inner[0].clone().into_parts();
        assert!(from.ones().collect_vec().is_empty());
        assert_eq!(to.ones().collect_vec(), &[1]);
    }

    #[test]
    fn test_add_fd_by_indices() {
        let mut fd = FunctionalDependencySet::new(4);
        fd.add_functional_dependency_by_column_indices(&[1, 2], &[0]); // (1, 2) --> (0), 4 columns
        let fd_inner = fd.into_dependencies();

        assert_eq!(fd_inner.len(), 1);

        let (from, to) = fd_inner[0].clone().into_parts();
        assert_eq!(from.ones().collect_vec(), &[1, 2]);
        assert_eq!(to.ones().collect_vec(), &[0]);
    }

    #[test]
    fn test_determined_by() {
        let mut fd = FunctionalDependencySet::new(5);
        fd.add_functional_dependency_by_column_indices(&[1, 2], &[0]); // (1, 2) --> (0)
        fd.add_functional_dependency_by_column_indices(&[0, 1], &[3]); // (0, 1) --> (3)
        fd.add_functional_dependency_by_column_indices(&[3], &[4]); // (3) --> (4)
        let from = FixedBitSet::from_iter([1usize, 2usize].into_iter());
        let to = FixedBitSet::from_iter([4usize].into_iter());
        assert!(fd.is_determined_by(&from, &to)); // (1, 2) --> (4) holds
    }
}
