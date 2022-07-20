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

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;
use risingwave_pb::catalog::Table;

#[derive(Default)]
struct LocalTableManagerCore {
    table_calalog_cache: HashMap<u32, Table>,
}

pub struct LocalTableManager {
    core: RwLock<LocalTableManagerCore>,
}

impl Default for LocalTableManager {
    fn default() -> Self {
        Self::new()
    }
}

impl LocalTableManager {
    pub fn new() -> Self {
        Self {
            core: RwLock::new(LocalTableManagerCore::default()),
        }
    }

    pub fn insert(&self, table_id: u32, table: Table) {
        let guard = &mut self.core.write().table_calalog_cache;
        guard.insert(table_id, table);
    }

    #[allow(dead_code)]
    pub fn erase(&self, table_id: u32) {
        let guard = &mut self.core.write().table_calalog_cache;
        guard.remove(&table_id);
    }

    #[allow(dead_code)]
    pub fn get(&self, table_id: u32) -> Option<Table> {
        let guard = &self.core.read().table_calalog_cache;
        guard.get(&table_id).cloned()
    }
}

pub type LocalTableManagerRef = Arc<LocalTableManager>;
