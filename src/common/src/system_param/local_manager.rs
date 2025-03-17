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

use std::ops::Deref;
use std::sync::Arc;

use arc_swap::ArcSwap;
use risingwave_pb::meta::SystemParams;
use tokio::sync::watch::{Receiver, Sender, channel};

use super::common::CommonHandler;
use super::diff::SystemParamsDiff;
use super::reader::SystemParamsReader;
use super::system_params_for_test;

pub type SystemParamsReaderRef = Arc<ArcSwap<SystemParamsReader>>;
pub type LocalSystemParamsManagerRef = Arc<LocalSystemParamsManager>;

/// The system parameter manager on worker nodes. It provides two methods for other components to
/// read the latest system parameters:
/// - `get_params` returns a reference to the latest parameters that is atomically updated.
/// - `watch_params` returns a channel on which calling `recv` will get the latest parameters.
///   Compared with `get_params`, the caller can be explicitly notified of parameter change.
#[derive(Debug)]
pub struct LocalSystemParamsManager {
    /// The latest parameters.
    params: SystemParamsReaderRef,

    /// Sender of the latest parameters.
    tx: Sender<SystemParamsReaderRef>,
}

impl LocalSystemParamsManager {
    /// Create a new instance of `LocalSystemParamsManager` and spawn a task to run
    /// the common handler.
    pub fn new(initial_params: SystemParamsReader) -> Self {
        let this = Self::new_inner(initial_params.clone());

        // Spawn a task to run the common handler.
        // TODO(bugen): this may be spawned multiple times under standalone deployment, though idempotent.
        tokio::spawn({
            let mut rx = this.tx.subscribe();
            async move {
                let mut params = initial_params.clone();
                let handler = CommonHandler::new(initial_params);

                while rx.changed().await.is_ok() {
                    // TODO: directly watch the changes instead of diffing ourselves.
                    let new_params = (**rx.borrow_and_update().load()).clone();
                    let diff = SystemParamsDiff::diff(params.as_ref(), new_params.as_ref());
                    handler.handle_change(&diff);
                    params = new_params;
                }
            }
        });

        this
    }

    pub fn for_test() -> Self {
        Self::new_inner(system_params_for_test().into())
    }

    fn new_inner(initial_params: SystemParamsReader) -> Self {
        let params = Arc::new(ArcSwap::from_pointee(initial_params));
        let (tx, _) = channel(params.clone());
        Self { params, tx }
    }

    pub fn get_params(&self) -> SystemParamsReaderRef {
        self.params.clone()
    }

    pub fn try_set_params(&self, new_params: SystemParams) {
        let new_params_reader = SystemParamsReader::from(new_params);
        if self.params.load().deref().deref() != &new_params_reader {
            self.params.store(Arc::new(new_params_reader));
            // Ignore no active receiver.
            let _ = self.tx.send(self.params.clone());
        }
    }

    pub fn watch_params(&self) -> Receiver<SystemParamsReaderRef> {
        self.tx.subscribe()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_manager() {
        let manager = LocalSystemParamsManager::for_test();
        let shared_params = manager.get_params();

        let new_params = SystemParams {
            sstable_size_mb: Some(114514),
            ..Default::default()
        };

        let mut params_rx = manager.watch_params();

        manager.try_set_params(new_params.clone());
        params_rx.changed().await.unwrap();
        assert_eq!(**params_rx.borrow().load(), new_params.clone().into());
        assert_eq!(**shared_params.load(), new_params.into());
    }
}
