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

use std::sync::{Arc, OnceLock};

use anyhow::anyhow;
use parking_lot::Mutex;
use sea_orm::DatabaseConnection;

use crate::sink::boxed::{BoxCoordinator, BoxLogSinker};
use crate::sink::{Sink, SinkError, SinkParam, SinkWriterParam};

pub trait BuildBoxLogSinkerTrait =
    FnMut(SinkParam, SinkWriterParam) -> BoxLogSinker + Send + 'static;
pub trait BuildBoxCoordinatorTrait = FnMut(DatabaseConnection) -> BoxCoordinator + Send + 'static;

type BuildBoxLogSinker = Box<dyn BuildBoxLogSinkerTrait>;
type BuildBoxCoordinator = Box<dyn BuildBoxCoordinatorTrait>;
pub const TEST_SINK_NAME: &str = "test";

#[derive(Debug)]
pub struct TestSink {
    param: SinkParam,
}

impl TryFrom<SinkParam> for TestSink {
    type Error = SinkError;

    fn try_from(param: SinkParam) -> Result<Self, Self::Error> {
        if cfg!(any(madsim, test)) {
            Ok(TestSink { param })
        } else {
            Err(SinkError::Config(anyhow!("test sink only support in test")))
        }
    }
}

impl Sink for TestSink {
    type Coordinator = BoxCoordinator;
    type LogSinker = BoxLogSinker;

    const SINK_NAME: &'static str = "test";

    async fn validate(&self) -> crate::sink::Result<()> {
        Ok(())
    }

    async fn new_log_sinker(
        &self,
        writer_param: SinkWriterParam,
    ) -> crate::sink::Result<Self::LogSinker> {
        Ok(build_box_log_sinker(self.param.clone(), writer_param))
    }

    async fn new_coordinator(
        &self,
        db: DatabaseConnection,
    ) -> crate::sink::Result<Self::Coordinator> {
        Ok(build_box_coordinator(db))
    }
}

struct TestSinkRegistry {
    build_box_sink: Arc<Mutex<Option<(BuildBoxLogSinker, BuildBoxCoordinator)>>>,
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

fn registry_build_sink_inner(
    build_box_log_sinker: impl BuildBoxLogSinkerTrait,
    build_box_coordinator: impl BuildBoxCoordinatorTrait,
) -> TestSinkRegistryGuard {
    assert!(
        get_registry()
            .build_box_sink
            .lock()
            .replace((
                Box::new(build_box_log_sinker),
                Box::new(build_box_coordinator)
            ))
            .is_none()
    );
    TestSinkRegistryGuard
}

pub fn registry_build_sink(
    build_box_log_sinker: impl BuildBoxLogSinkerTrait,
) -> TestSinkRegistryGuard {
    registry_build_sink_inner(build_box_log_sinker, |_| {
        unreachable!("no coordinator registered")
    })
}

fn build_box_coordinator(db: DatabaseConnection) -> BoxCoordinator {
    (get_registry()
        .build_box_sink
        .lock()
        .as_mut()
        .expect("should not be empty")
        .1)(db)
}

fn build_box_log_sinker(param: SinkParam, writer_param: SinkWriterParam) -> BoxLogSinker {
    (get_registry()
        .build_box_sink
        .lock()
        .as_mut()
        .expect("should not be empty")
        .0)(param, writer_param)
}
