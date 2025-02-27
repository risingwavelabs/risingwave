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

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicU32;

use parking_lot::RwLock;

pub type Count = Arc<AtomicU32>;

#[derive(Clone)]
pub struct CountMap(Arc<RwLock<HashMap<u32, Count>>>);

impl CountMap {
    pub(crate) fn new() -> Self {
        CountMap(Arc::new(RwLock::new(HashMap::new())))
    }

    pub(crate) fn new_counter(&self, id: u32) -> Count {
        let mut map = self.0.write();
        let counter = Arc::new(AtomicU32::new(0));
        map.insert(id, counter.clone());
        counter
    }
}
