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

#[derive(Debug, EnumAsInner)]
pub enum JoinRowSet<K, V> {
    BTree(BTreeMap<K, V>),
    Vec(Vec<(K, V)>),
}

impl<K, V> Default for JoinRowSet<K, V> {
    fn default() -> Self {
        Self::Vec(vec![])
    }
}

pub struct VecOccupiedError<'a, K, V> {
    key: &'a K,
    old_value: &'a V,
    new_value: V,
}

impl<'a, K: Debug, V: Debug> Debug for VecOccupiedError<'a, K, V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VecOccupiedError")
            .field("key", &self.key)
            .field("old_value", &self.old_value)
            .field("new_value", &self.new_value)
            .finish()
    }
}

#[derive(Debug)]
pub enum JoinRowSetOccupiedError<'a, K: Ord, V> {
    BTree(BTreeMapOccupiedError<'a, K, V>),
    Vec(VecOccupiedError<'a, K, V>),
}

impl<K: Ord + Debug, V: Debug> JoinRowSet<K, V> {
    pub fn try_insert(
        &mut self,
        key: K,
        value: V,
    ) -> Result<&'_ mut V, JoinRowSetOccupiedError<'_, K, V>> {
        const MAX_VEC_SIZE: usize = 4;

        if let Self::Vec(inner) = self && inner.len() >= MAX_VEC_SIZE{
            let btree = BTreeMap::from_iter(inner.drain(..));
            mem::swap(self, &mut Self::BTree(btree));
        }

        match self {
            Self::BTree(inner) => inner
                .try_insert(key, value)
                .map_err(JoinRowSetOccupiedError::BTree),
            Self::Vec(inner) => {
                if let Some(pos) = inner.iter().position(|elem| elem.0 == key) {
                    return Err(JoinRowSetOccupiedError::Vec(VecOccupiedError {
                        key: &inner[pos].0,
                        old_value: &inner[pos].1,
                        new_value: value,
                    }));
                } else {
                    inner.push((key, value));
                    return Ok(&mut inner.last_mut().unwrap().1);
                }
            }
        }
    }

    pub fn remove(&mut self, key: &K) -> Option<V> {
        match self {
            Self::BTree(inner) => inner.remove(key),
            Self::Vec(inner) => inner
                .iter()
                .position(|elem| &elem.0 == key)
                .map(|pos| inner.remove(pos).1),
        }
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
            Self::BTree(inner) => inner.iter_mut().map(|(_, v)| v),
            Self::Vec(inner) => inner.iter_mut().map(|(_, v)| v),
        }
    }
}
