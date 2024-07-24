// Copyright 2024 RisingWave Labs
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

use tokio::sync::watch::{self, Receiver, Sender};

use crate::{ElectionClient, ElectionMember, MetaResult};

/// A dummy implementation of [`ElectionClient`] for scenarios where only one meta node is running,
/// typically for testing purposes such as an in-memory meta store.
///
/// This can be used to unify the code paths no matter there's HA or not.
pub struct DummyElectionClient {
    id: String,

    /// A dummy watcher that never changes, indicating we are always the leader.
    dummy_watcher: Sender<bool>,
}

impl DummyElectionClient {
    pub fn new(id: String) -> Self {
        Self {
            id,
            dummy_watcher: watch::channel(true).0,
        }
    }

    fn self_member(&self) -> ElectionMember {
        ElectionMember {
            id: self.id.clone(),
            is_leader: true,
        }
    }
}

#[async_trait::async_trait]
impl ElectionClient for DummyElectionClient {
    fn id(&self) -> MetaResult<String> {
        Ok(self.id.clone())
    }

    async fn run_once(&self, _ttl: i64, mut stop: Receiver<()>) -> MetaResult<()> {
        // Only exit when the stop signal is received.
        let _ = stop.changed().await;
        Ok(())
    }

    fn subscribe(&self) -> Receiver<bool> {
        self.dummy_watcher.subscribe()
    }

    async fn leader(&self) -> MetaResult<Option<ElectionMember>> {
        Ok(Some(self.self_member()))
    }

    async fn get_members(&self) -> MetaResult<Vec<ElectionMember>> {
        Ok(vec![self.self_member()])
    }

    fn is_leader(&self) -> bool {
        true
    }
}
