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

use std::ops::Deref;
use std::sync::Arc;

use arc_swap::ArcSwap;
use risingwave_pb::meta::SystemParams;
use tokio::sync::watch::{channel, Receiver, Sender};

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
    pub fn new(params: SystemParamsReader) -> Self {
        let params = Arc::new(ArcSwap::from_pointee(params));
        let (tx, _) = channel(params.clone());
        Self { params, tx }
    }

    pub fn for_test() -> Self {
        Self::new(system_params_for_test().into())
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
        let p = SystemParams::default().into();
        let manager = LocalSystemParamsManager::new(p);
        let shared_params = manager.get_params();

        let new_params = SystemParams {
            sstable_size_mb: Some(1),
            ..Default::default()
        };

        let mut params_rx = manager.watch_params();

        manager.try_set_params(new_params.clone());
        params_rx.changed().await.unwrap();
        assert_eq!(**params_rx.borrow().load(), new_params.clone().into());
        assert_eq!(**shared_params.load(), new_params.into());
    }
}
