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

use std::collections::{HashMap, HashSet};

use dyn_clone::DynClone;
use risingwave_hummock_sdk::key::FullKey;

pub trait CompactionFilter: Send + Sync + DynClone {
    fn should_delete(&mut self, _: FullKey<&[u8]>) -> bool {
        false
    }
}

dyn_clone::clone_trait_object!(CompactionFilter);

#[derive(Clone)]
pub struct DummyCompactionFilter;

impl CompactionFilter for DummyCompactionFilter {}

#[derive(Clone)]
pub struct StateCleanUpCompactionFilter {
    existing_table_ids: HashSet<u32>,
    last_table: Option<(u32, bool)>,
}

impl StateCleanUpCompactionFilter {
    pub fn new(table_id_set: HashSet<u32>) -> Self {
        StateCleanUpCompactionFilter {
            existing_table_ids: table_id_set,
            last_table: None,
        }
    }
}

impl CompactionFilter for StateCleanUpCompactionFilter {
    fn should_delete(&mut self, key: FullKey<&[u8]>) -> bool {
        let table_id = key.user_key.table_id.table_id();
        if let Some((last_table_id, removed)) = self.last_table.as_ref() {
            if *last_table_id == table_id {
                return *removed;
            }
        }
        let removed = !self.existing_table_ids.contains(&table_id);
        self.last_table = Some((table_id, removed));
        removed
    }
}

#[derive(Clone)]
pub struct TtlCompactionFilter {
    table_id_to_ttl: HashMap<u32, u32>,
    expire_epoch: u64,
    last_table_and_ttl: Option<(u32, u64)>,
}

impl CompactionFilter for TtlCompactionFilter {
    fn should_delete(&mut self, key: FullKey<&[u8]>) -> bool {
        pub use risingwave_common::util::epoch::Epoch;
        let table_id = key.user_key.table_id.table_id();
        let epoch = key.epoch_with_gap.pure_epoch();
        if let Some((last_table_id, ttl_mill)) = self.last_table_and_ttl.as_ref() {
            if *last_table_id == table_id {
                let min_epoch = Epoch(self.expire_epoch).subtract_ms(*ttl_mill);
                return Epoch(epoch) < min_epoch;
            }
        }
        match self.table_id_to_ttl.get(&table_id) {
            Some(ttl_second_u32) => {
                assert!(*ttl_second_u32 > 0);
                // default to zero.
                let ttl_mill = *ttl_second_u32 as u64 * 1000;
                let min_epoch = Epoch(self.expire_epoch).subtract_ms(ttl_mill);
                self.last_table_and_ttl = Some((table_id, ttl_mill));
                Epoch(epoch) < min_epoch
            }
            None => false,
        }
    }
}

impl TtlCompactionFilter {
    pub fn new(table_id_to_ttl: HashMap<u32, u32>, expire: u64) -> Self {
        Self {
            table_id_to_ttl,
            expire_epoch: expire,
            last_table_and_ttl: None,
        }
    }
}

#[derive(Default, Clone)]
pub struct MultiCompactionFilter {
    filter_vec: Vec<Box<dyn CompactionFilter>>,
}

impl CompactionFilter for MultiCompactionFilter {
    fn should_delete(&mut self, key: FullKey<&[u8]>) -> bool {
        self.filter_vec
            .iter_mut()
            .any(|filter| filter.should_delete(key))
    }
}

impl MultiCompactionFilter {
    pub fn register(&mut self, filter: Box<dyn CompactionFilter>) {
        self.filter_vec.push(filter);
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use risingwave_common::catalog::TableId;
    use risingwave_common::util::epoch::test_epoch;
    use risingwave_hummock_sdk::key::{FullKey, TableKey};

    use super::{CompactionFilter, TtlCompactionFilter};

    #[test]
    fn test_ttl_u32() {
        let mut ttl_filter = TtlCompactionFilter::new(HashMap::from_iter([(1, 4000000000)]), 1);
        ttl_filter
            .should_delete(FullKey::new(TableId::new(1), TableKey(vec![]), test_epoch(1)).to_ref());
    }
}
