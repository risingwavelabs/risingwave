// Copyright 2025 RisingWave Labs
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

use std::fmt;

use fixedbitset::FixedBitSet;
use itertools::Itertools;

use crate::optimizer::property::Order;

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
#[derive(Debug, PartialEq, Eq, Hash, Clone, Default)]
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
        self.strict.iter().format(", ").fmt(f)?;
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

    /// O(d), where `d` is the number of functional dependencies.
    /// The call to `is_subset` is technically O(n),
    /// but we can consider it O(1) since the constant factor is 1/usize.
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

    /// Return `true` if the dependency determinant -> dependent exists.
    /// O(d), where the dominant cost is from `Self::get_closure`, which has O(d) complexity.
    pub fn is_determined_by(&self, determinant: &FixedBitSet, dependent: &FixedBitSet) -> bool {
        self.get_closure(determinant).is_superset(dependent)
    }

    /// This just checks if the columns specified by the `columns` bitset
    /// determines each other column.
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

    /// This is just a wrapper around `Self::minimize_key_bitset` to minimize `key_indices`.
    pub fn minimize_key(&self, key_indices: &[usize]) -> Vec<usize> {
        let mut key_bitset = FixedBitSet::from_iter(key_indices.iter().copied());
        key_bitset.grow(self.column_count);
        let res = self.minimize_key_bitset(key_bitset);
        res.ones().collect_vec()
    }

    /// -------
    /// Overview
    /// -------
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
    ///
    /// ---------
    /// Algorithm
    /// ---------
    /// This algorithm removes columns one by one and check
    /// whether the remaining columns can form a key or not. If the remaining columns can form a
    /// key, then this column can be removed.
    fn minimize_key_bitset(&self, key: FixedBitSet) -> FixedBitSet {
        assert!(
            self.is_key_inner(&key),
            "{:?} is not a key!",
            key.ones().collect_vec()
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

    /// Wrapper around `Self::minimize_order_key_bitset` to minimize `order_key`.
    /// View the documentation of `Self::minimize_order_key_bitset` for more information.
    /// In the process of minimizing the order key,
    /// we must ensure that if the indices are part of
    /// distribution key, they must not be pruned.
    pub fn minimize_order_key(&self, order_key: Order, dist_key_indices: &[usize]) -> Order {
        let dist_key_bitset = FixedBitSet::from_iter(dist_key_indices.iter().copied());
        let order_key_indices = order_key
            .column_orders
            .iter()
            .map(|o| o.column_index)
            .collect();
        let min_bitset = self.minimize_order_key_bitset(order_key_indices);
        let order = order_key
            .column_orders
            .iter()
            .filter(|o| {
                min_bitset.contains(o.column_index) || dist_key_bitset.contains(o.column_index)
            })
            .cloned()
            .collect_vec();
        Order::new(order)
    }

    /// 1. Iterate over the prefixes of the order key.
    /// 2. If some continuous subset of columns and the next
    ///    column of the order key form a functional dependency,
    ///    we can prune that column.
    /// 3. This function has O(dn) complexity, where:
    ///    1. `d` is the number of functional dependencies,
    ///       because each iteration in the loop calls `Self::is_determined_by`.
    ///    2. `n` is the number of columns.
    fn minimize_order_key_bitset(&self, order_key: Vec<usize>) -> FixedBitSet {
        if order_key.is_empty() {
            return FixedBitSet::new();
        }

        // Initialize current_prefix.
        let mut prefix = FixedBitSet::with_capacity(self.column_count);

        // Grow current_prefix.
        for i in order_key {
            let mut next = FixedBitSet::with_capacity(self.column_count);
            next.set(i, true);

            // Check if prefix -> next_column
            if !self.is_determined_by(&prefix, &next) {
                prefix.set(i, true);
            }
        }
        prefix
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
        let from = FixedBitSet::from_iter([1usize, 2usize]);
        let to = FixedBitSet::from_iter([4usize]);
        assert!(fd.is_determined_by(&from, &to)); // (1, 2) --> (4) holds
    }

    // (1, 2) -> (0)
    // [1, 2, 0] -> [1, 2] (prune, since 0 is after continuous (1, 2))
    #[test]
    fn test_minimize_order_by_prefix() {
        let mut fd = FunctionalDependencySet::new(5);
        fd.add_functional_dependency_by_column_indices(&[1, 2], &[0]); // (1, 2) --> (0)
        let order_key = vec![1usize, 2usize, 0usize];
        let actual_key = fd.minimize_order_key_bitset(order_key);
        let mut expected_key = FixedBitSet::with_capacity(5);
        expected_key.set(1, true);
        expected_key.set(2, true);
        println!("{:b}", actual_key);
        println!("{:b}", expected_key);
        assert_eq!(actual_key, expected_key);
    }

    // (1, 2) -> (0)
    // [3, 1, 2, 0] -> [3, 1, 2] (prune, since 0 is after continuous (1, 2))
    #[test]
    fn test_minimize_order_by_tail_subset_prefix() {
        let mut fd = FunctionalDependencySet::new(5);
        fd.add_functional_dependency_by_column_indices(&[1, 2], &[0]); // (1, 2) --> (0)
        let order_key = vec![3usize, 1usize, 2usize, 0usize];
        let actual_key = fd.minimize_order_key_bitset(order_key);
        let mut expected_key = FixedBitSet::with_capacity(5);
        expected_key.set(1, true);
        expected_key.set(2, true);
        expected_key.set(3, true);
        println!("{:b}", actual_key);
        println!("{:b}", expected_key);
        assert_eq!(actual_key, expected_key);
    }

    // (1, 2) -> (0)
    // [3, 1, 2, 4, 0] -> [3, 1, 2, 4] (prune, since continuous (1, 2) is before 0)
    #[test]
    fn test_minimize_order_by_middle_subset_prefix() {
        let mut fd = FunctionalDependencySet::new(5);
        fd.add_functional_dependency_by_column_indices(&[1, 2], &[0]); // (1, 2) --> (0)
        let order_key = vec![3usize, 1usize, 2usize, 4usize, 0usize];
        let actual_key = fd.minimize_order_key_bitset(order_key);
        let mut expected_key = FixedBitSet::with_capacity(5);
        expected_key.set(1, true);
        expected_key.set(2, true);
        expected_key.set(3, true);
        expected_key.set(4, true);
        println!("{:b}", actual_key);
        println!("{:b}", expected_key);
        assert_eq!(actual_key, expected_key);
    }

    // (1, 2) -> (0)
    // [0, 1, 2] -> [0, 1, 2] (no pruning)
    #[test]
    fn test_minimize_order_by_suffix_cant_prune_prefix() {
        let mut fd = FunctionalDependencySet::new(5);
        fd.add_functional_dependency_by_column_indices(&[1, 2], &[0]); // (1, 2) --> (0)
        let order_key = vec![0usize, 1usize, 2usize];
        let actual_key = fd.minimize_order_key_bitset(order_key);
        let mut expected_key = FixedBitSet::with_capacity(5);
        expected_key.set(0, true);
        expected_key.set(1, true);
        expected_key.set(2, true);
        println!("{:b}", actual_key);
        println!("{:b}", expected_key);
        assert_eq!(actual_key, expected_key);
    }

    #[test]
    fn test_minimize_order_by_with_empty_key() {
        let mut fd = FunctionalDependencySet::new(5);
        fd.add_functional_dependency_by_column_indices(&[], &[0]); // () --> (0)
        let order_key = vec![0];
        let actual_key = fd.minimize_order_key_bitset(order_key);
        let expected_key = FixedBitSet::with_capacity(5);
        println!("{:b}", actual_key);
        println!("{:b}", expected_key);
        assert_eq!(actual_key, expected_key);
    }
}
