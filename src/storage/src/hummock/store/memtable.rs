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

use std::collections::BTreeMap;
use std::ops::RangeBounds;

use bytes::Bytes;

macro_rules! memtable_impl_method_body {
    ($memtable:expr, $method_name:ident, $($args:expr),+) => {
        {
            match $memtable {
                Memtable::BTree(inner) => {
                    inner.$method_name($($args),+)
                },
            }
        }
    };
}

// TODO: may want to change the return type to avoid cloning Bytes.
pub type MemtableIter = impl Iterator<Item = (Bytes, Bytes)>;

pub enum Memtable {
    BTree(BTreeMapMemtable),
}

#[expect(dead_code)]
impl Memtable {
    /// Inserts a key-value entry associated with a given `epoch` into memtable.
    fn insert(&mut self, key: Bytes, val: Bytes, epoch: u64) {
        memtable_impl_method_body!(self, insert, key, val, epoch)
    }

    /// Deletes a key-value entry from memtable. Only the key-value entry with epoch smaller
    /// than the given `epoch` will be deleted.
    fn delete(&mut self, key: Bytes, epoch: u64) {
        memtable_impl_method_body!(self, delete, key, epoch)
    }

    /// Point gets a value from memtable.
    /// The result is based on a snapshot corresponding to the given `epoch`.
    fn get(&self, key: &[u8], epoch: u64) -> Option<Bytes> {
        memtable_impl_method_body!(self, get, key, epoch)
    }

    /// Opens and returns an iterator for a given `key_range`.
    /// The returned iterator will iterate data based on a snapshot corresponding to
    /// the given `epoch`.
    fn iter<R, B>(&self, key_range: R) -> MemtableIter
    where
        R: RangeBounds<B> + Send,
        B: AsRef<[u8]> + Send,
    {
        memtable_impl_method_body!(self, iter, key_range)
    }
}

#[expect(dead_code)]
pub struct BTreeMapMemtable {
    mem: BTreeMap<Bytes, Bytes>,
}

#[expect(unused_variables, dead_code)]
impl BTreeMapMemtable {
    fn insert(&mut self, key: Bytes, val: Bytes, epoch: u64) {
        unimplemented!()
    }

    fn delete(&mut self, key: Bytes, epoch: u64) {
        unimplemented!()
    }

    fn get(&self, key: &[u8], epoch: u64) -> Option<Bytes> {
        unimplemented!()
    }

    fn iter<R, B>(&self, key_range: R) -> MemtableIter
    where
        R: RangeBounds<B> + Send,
        B: AsRef<[u8]> + Send,
    {
        std::iter::empty()
    }
}
