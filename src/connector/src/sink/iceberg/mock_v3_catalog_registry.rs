// Copyright 2026 RisingWave Labs
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

//! Test-only registry for injecting a mock `iceberg::Catalog` into the V3 sink
//! commit path. Used by V3 2PC fault-injection tests.
//!
//! Activated when `catalog_type = 'mock_v3'` is set on a CREATE SINK statement.

use std::sync::{Arc, Mutex, OnceLock};

use iceberg::Catalog;

static REGISTRY: OnceLock<Mutex<Option<Arc<dyn Catalog>>>> = OnceLock::new();

fn cell() -> &'static Mutex<Option<Arc<dyn Catalog>>> {
    REGISTRY.get_or_init(|| Mutex::new(None))
}

/// Drop-guard: clears the registered mock catalog when the guard is dropped.
pub struct MockCatalogGuard;

impl Drop for MockCatalogGuard {
    fn drop(&mut self) {
        *cell().lock().unwrap() = None;
    }
}

/// Register a global mock catalog. `IcebergConfig::create_catalog` will return
/// this catalog when `catalog_type == "mock_v3"`.
///
/// Only one mock may be registered at a time; double registration panics so
/// tests cannot accidentally interfere with each other.
pub fn register(cat: Arc<dyn Catalog>) -> MockCatalogGuard {
    let mut slot = cell().lock().unwrap();
    if slot.is_some() {
        panic!("mock_v3 catalog already registered; previous MockCatalogGuard was not dropped");
    }
    *slot = Some(cat);
    MockCatalogGuard
}

/// Get the currently registered mock catalog, if any.
pub fn get() -> Option<Arc<dyn Catalog>> {
    cell().lock().unwrap().clone()
}
