// Copyright 2024 RisingWave Labs
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

    pub fn remove<Q>(&mut self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let ret = match self {
            Self::BTree(inner) => inner.remove(key),
            Self::Vec(inner) => inner
                .iter()
                .position(|elem| elem.0.borrow() == key)
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
    pub fn values(&self) -> impl Iterator<Item = &V> {
        match self {
            Self::BTree(inner) => inner.values(),
            Self::Vec(inner) => inner.iter().map(|(_, v)| v),
        }
    }

    #[auto_enum(Iterator)]
    pub fn values_mut(&mut self) -> impl Iterator<Item = &'_ mut V> {
        match self {
            Self::BTree(inner) => inner.values_mut(),
            Self::Vec(inner) => inner.iter_mut().map(|(_, v)| v),
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

    /// Returns the smallest and second-smallest keys in the map.
    pub fn first_two_key_sorted(&self) -> (Option<&K>, Option<&K>) {
        match self {
            Self::BTree(inner) => {
                let mut iter = inner.keys();
                (iter.next(), iter.next())
            }
            Self::Vec(inner) => {
                let mut smallest = None;
                let mut second = None;
                for (k, _) in inner {
                    if let Some(smallest_k) = smallest {
                        if k < smallest_k {
                            second = Some(smallest_k);
                            smallest = Some(k);
                        } else if second.is_none_or(|s| k < s) {
                            second = Some(k);
                        }
                    } else {
                        smallest = Some(k);
                    }
                }
                (smallest, second)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_join_row_set_first_two_key_sorted() {
        {
            let mut join_row_set: JoinRowSet<i32, i32> = JoinRowSet::default();

            // Insert elements
            assert!(join_row_set.try_insert(3, 30).is_ok());
            assert!(join_row_set.try_insert(1, 10).is_ok());
            assert!(join_row_set.try_insert(2, 20).is_ok());

            assert_eq!(join_row_set.first_two_key_sorted(), (Some(&1), Some(&2)));
        }
        {
            let mut join_row_set: JoinRowSet<i32, i32> = JoinRowSet::default();

            // Insert elements
            assert!(join_row_set.try_insert(1, 10).is_ok());
            assert!(join_row_set.try_insert(2, 20).is_ok());

            assert_eq!(join_row_set.first_two_key_sorted(), (Some(&1), Some(&2)));
        }
        {
            let mut join_row_set: JoinRowSet<i32, i32> = JoinRowSet::default();

            assert!(join_row_set.try_insert(1, 10).is_ok());

            assert_eq!(join_row_set.first_two_key_sorted(), (Some(&1), None));
        }
    }
}
