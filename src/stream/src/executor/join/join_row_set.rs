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

use std::borrow::Borrow;
use std::collections::BTreeMap;
use std::collections::btree_map::OccupiedError as BTreeMapOccupiedError;
use std::fmt::Debug;
use std::mem;
use std::ops::{Bound, RangeBounds};

use auto_enums::auto_enum;
use enum_as_inner::EnumAsInner;

const MAX_VEC_SIZE: usize = 4;

#[derive(Debug, EnumAsInner)]
pub enum JoinRowSet<K, V> {
    BTree(BTreeMap<K, V>),
    Vec(Vec<(K, V)>),
}

impl<K, V> Default for JoinRowSet<K, V> {
    fn default() -> Self {
        Self::Vec(Vec::new())
    }
}

#[derive(Debug)]
#[allow(dead_code)]
pub struct VecOccupiedError<'a, K, V> {
    key: &'a K,
    old_value: &'a V,
    new_value: V,
}

#[derive(Debug)]
pub enum JoinRowSetOccupiedError<'a, K: Ord, V> {
    BTree(BTreeMapOccupiedError<'a, K, V>),
    Vec(VecOccupiedError<'a, K, V>),
}

impl<K: Ord, V> JoinRowSet<K, V> {
    pub fn try_insert(
        &mut self,
        key: K,
        value: V,
    ) -> Result<&'_ mut V, JoinRowSetOccupiedError<'_, K, V>> {
        if let Self::Vec(inner) = self
            && inner.len() >= MAX_VEC_SIZE
        {
            let btree = BTreeMap::from_iter(inner.drain(..));
            *self = Self::BTree(btree);
        }

        match self {
            Self::BTree(inner) => inner
                .try_insert(key, value)
                .map_err(JoinRowSetOccupiedError::BTree),
            Self::Vec(inner) => {
                if let Some(pos) = inner.iter().position(|elem| elem.0 == key) {
                    Err(JoinRowSetOccupiedError::Vec(VecOccupiedError {
                        key: &inner[pos].0,
                        old_value: &inner[pos].1,
                        new_value: value,
                    }))
                } else {
                    if inner.capacity() == 0 {
                        // `Vec` will give capacity 4 when `1 < mem::size_of::<T> <= 1024`
                        // We only give one for memory optimization
                        inner.reserve_exact(1);
                    }
                    inner.push((key, value));
                    Ok(&mut inner.last_mut().unwrap().1)
                }
            }
        }
    }

    pub fn remove(&mut self, key: &K) -> Option<V> {
        let ret = match self {
            Self::BTree(inner) => inner.remove(key),
            Self::Vec(inner) => inner
                .iter()
                .position(|elem| &elem.0 == key)
                .map(|pos| inner.swap_remove(pos).1),
        };
        if let Self::BTree(inner) = self
            && inner.len() <= MAX_VEC_SIZE / 2
        {
            let btree = mem::take(inner);
            let vec = Vec::from_iter(btree);
            *self = Self::Vec(vec);
        }
        ret
    }

    pub fn len(&self) -> usize {
        match self {
            Self::BTree(inner) => inner.len(),
            Self::Vec(inner) => inner.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            Self::BTree(inner) => inner.is_empty(),
            Self::Vec(inner) => inner.is_empty(),
        }
    }

    #[auto_enum(Iterator)]
    pub fn values_mut(&mut self) -> impl Iterator<Item = &'_ mut V> {
        match self {
            Self::BTree(inner) => inner.values_mut(),
            Self::Vec(inner) => inner.iter_mut().map(|(_, v)| v),
        }
    }

    #[auto_enum(Iterator)]
    pub fn keys(&self) -> impl Iterator<Item = &K> {
        match self {
            Self::BTree(inner) => inner.keys(),
            Self::Vec(inner) => inner.iter().map(|(k, _v)| k),
        }
    }

    #[auto_enum(Iterator)]
    pub fn range<T, R>(&self, range: R) -> impl Iterator<Item = (&K, &V)>
    where
        T: Ord + ?Sized,
        K: Borrow<T> + Ord,
        R: RangeBounds<T>,
    {
        match self {
            Self::BTree(inner) => inner.range(range),
            Self::Vec(inner) => inner
                .iter()
                .filter(move |(k, _)| range.contains(k.borrow()))
                .map(|(k, v)| (k, v)),
        }
    }

    pub fn lower_bound_key(&self, bound: Bound<&K>) -> Option<&K> {
        self.lower_bound(bound).map(|(k, _v)| k)
    }

    pub fn upper_bound_key(&self, bound: Bound<&K>) -> Option<&K> {
        self.upper_bound(bound).map(|(k, _v)| k)
    }

    pub fn lower_bound(&self, bound: Bound<&K>) -> Option<(&K, &V)> {
        match self {
            Self::BTree(inner) => inner.lower_bound(bound).next(),
            Self::Vec(inner) => inner
                .iter()
                .filter(|(k, _)| (bound, Bound::Unbounded).contains(k))
                .min_by_key(|(k, _)| k)
                .map(|(k, v)| (k, v)),
        }
    }

    pub fn upper_bound(&self, bound: Bound<&K>) -> Option<(&K, &V)> {
        match self {
            Self::BTree(inner) => inner.upper_bound(bound).prev(),
            Self::Vec(inner) => inner
                .iter()
                .filter(|(k, _)| (Bound::Unbounded, bound).contains(k))
                .max_by_key(|(k, _)| k)
                .map(|(k, v)| (k, v)),
        }
    }

    pub fn get_mut(&mut self, key: &K) -> Option<&mut V> {
        match self {
            Self::BTree(inner) => inner.get_mut(key),
            Self::Vec(inner) => inner.iter_mut().find(|(k, _)| k == key).map(|(_, v)| v),
        }
    }

    pub fn get(&self, key: &K) -> Option<&V> {
        match self {
            Self::BTree(inner) => inner.get(key),
            Self::Vec(inner) => inner.iter().find(|(k, _)| k == key).map(|(_, v)| v),
        }
    }

    /// Returns the key-value pair with smallest key in the map.
    pub fn first_key_sorted(&self) -> Option<&K> {
        match self {
            Self::BTree(inner) => inner.first_key_value().map(|(k, _)| k),
            Self::Vec(inner) => inner.iter().map(|(k, _)| k).min(),
        }
    }

    /// Returns the key-value pair with the second smallest key in the map.
    pub fn second_key_sorted(&self) -> Option<&K> {
        match self {
            Self::BTree(inner) => inner.iter().nth(1).map(|(k, _)| k),
            Self::Vec(inner) => {
                let mut res = None;
                let mut smallest = None;
                for (k, _) in inner {
                    if let Some(smallest_k) = smallest {
                        if k < smallest_k {
                            res = Some(smallest_k);
                            smallest = Some(k);
                        } else if let Some(res_k) = res {
                            if k < res_k {
                                res = Some(k);
                            }
                        } else {
                            res = Some(k);
                        }
                    } else {
                        smallest = Some(k);
                    }
                }
                res
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_join_row_set_bounds() {
        let mut join_row_set: JoinRowSet<i32, i32> = JoinRowSet::default();

        // Insert elements
        assert!(join_row_set.try_insert(1, 10).is_ok());
        assert!(join_row_set.try_insert(2, 20).is_ok());
        assert!(join_row_set.try_insert(3, 30).is_ok());

        // Check lower bound
        assert_eq!(join_row_set.lower_bound_key(Bound::Included(&2)), Some(&2));
        assert_eq!(join_row_set.lower_bound_key(Bound::Excluded(&2)), Some(&3));

        // Check upper bound
        assert_eq!(join_row_set.upper_bound_key(Bound::Included(&2)), Some(&2));
        assert_eq!(join_row_set.upper_bound_key(Bound::Excluded(&2)), Some(&1));
    }

    #[test]
    fn test_join_row_set_first_and_second_key_sorted() {
        {
            let mut join_row_set: JoinRowSet<i32, i32> = JoinRowSet::default();

            // Insert elements
            assert!(join_row_set.try_insert(3, 30).is_ok());
            assert!(join_row_set.try_insert(1, 10).is_ok());
            assert!(join_row_set.try_insert(2, 20).is_ok());

            // Check first key sorted
            assert_eq!(join_row_set.first_key_sorted(), Some(&1));

            // Check second key sorted
            assert_eq!(join_row_set.second_key_sorted(), Some(&2));
        }
        {
            let mut join_row_set: JoinRowSet<i32, i32> = JoinRowSet::default();

            // Insert elements
            assert!(join_row_set.try_insert(1, 10).is_ok());
            assert!(join_row_set.try_insert(2, 20).is_ok());

            // Check first key sorted
            assert_eq!(join_row_set.first_key_sorted(), Some(&1));

            // Check second key sorted
            assert_eq!(join_row_set.second_key_sorted(), Some(&2));
        }
    }
}
