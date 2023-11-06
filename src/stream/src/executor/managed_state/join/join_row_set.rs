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

use std::collections::btree_map::OccupiedError as BTreeMapOccupiedError;
use std::collections::BTreeMap;
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
        if let Self::Vec(inner) = self && inner.len() >= MAX_VEC_SIZE {
            let btree = BTreeMap::from_iter(inner.drain(..));
            mem::swap(self, &mut Self::BTree(btree));
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
        if let Self::BTree(inner) = self && inner.len() <= MAX_VEC_SIZE / 2 {
            let btree = mem::take(inner);
            let vec = Vec::from_iter(btree);
            mem::swap(self, &mut Self::Vec(vec));
        }
        ret
    }

    pub fn len(&self) -> usize {
        match self {
            Self::BTree(inner) => inner.len(),
            Self::Vec(inner) => inner.len(),
        }
    }

    #[auto_enum(Iterator)]
    pub fn values_mut(&mut self) -> impl Iterator<Item = &'_ mut V> {
        match self {
            Self::BTree(inner) => inner.values_mut(),
            Self::Vec(inner) => inner.iter_mut().map(|(_, v)| v),
        }
    }
}
