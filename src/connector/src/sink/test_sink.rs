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

use std::sync::{Arc, OnceLock};

use parking_lot::Mutex;

use crate::sink::boxed::BoxSink;
use crate::sink::{SinkError, SinkParam};

pub type BuildBoxSink =
    Box<dyn Fn(SinkParam) -> Result<BoxSink, SinkError> + Send + Sync + 'static>;
pub const TEST_SINK_NAME: &str = "test";

struct TestSinkRegistry {
    build_box_sink: Arc<Mutex<Option<BuildBoxSink>>>,
}

impl TestSinkRegistry {
    fn new() -> Self {
        TestSinkRegistry {
            build_box_sink: Arc::new(Mutex::new(None)),
        }
    }
}

fn get_registry() -> &'static TestSinkRegistry {
    static GLOBAL_REGISTRY: OnceLock<TestSinkRegistry> = OnceLock::new();
    GLOBAL_REGISTRY.get_or_init(TestSinkRegistry::new)
}

pub struct TestSinkRegistryGuard;

impl Drop for TestSinkRegistryGuard {
    fn drop(&mut self) {
        assert!(get_registry().build_box_sink.lock().take().is_some());
    }
}

pub fn registry_build_sink(
    build_sink: impl Fn(SinkParam) -> Result<BoxSink, SinkError> + Send + Sync + 'static,
) -> TestSinkRegistryGuard {
    assert!(get_registry()
        .build_box_sink
        .lock()
        .replace(Box::new(build_sink))
        .is_none());
    TestSinkRegistryGuard
}

pub fn build_test_sink(param: SinkParam) -> Result<BoxSink, SinkError> {
    (get_registry()
        .build_box_sink
        .lock()
        .as_ref()
        .expect("should not be empty"))(param)
}
