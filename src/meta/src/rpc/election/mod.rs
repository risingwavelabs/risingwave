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
pub mod etcd;
pub mod sql;

use serde::Serialize;
use tokio::sync::watch::{self, Receiver};

use crate::MetaResult;

const META_ELECTION_KEY: &str = "__meta_election_";

#[derive(Debug, Serialize)]
pub struct ElectionMember {
    pub id: String,
    pub is_leader: bool,
}

#[async_trait::async_trait]
pub trait ElectionClient: Send + Sync + 'static {
    async fn init(&self) -> MetaResult<()> {
        Ok(())
    }

    fn id(&self) -> MetaResult<String>;
    async fn run_once(&self, ttl: i64, stop: Receiver<()>) -> MetaResult<()>;
    fn subscribe(&self) -> Receiver<bool>;
    async fn leader(&self) -> MetaResult<Option<ElectionMember>>;
    async fn get_members(&self) -> MetaResult<Vec<ElectionMember>>;
    async fn is_leader(&self) -> bool;
}

pub struct DummyElectionClient {
    id: String,
    dummy_channel: watch::Sender<bool>,
}

impl DummyElectionClient {
    pub fn new(id: String) -> Self {
        let dummy_channel = watch::channel(true).0;
        Self { id, dummy_channel }
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

    async fn run_once(&self, _ttl: i64, _stop: Receiver<()>) -> MetaResult<()> {
        futures::future::pending().await
    }

    fn subscribe(&self) -> Receiver<bool> {
        self.dummy_channel.subscribe()
    }

    async fn leader(&self) -> MetaResult<Option<ElectionMember>> {
        Ok(Some(self.self_member()))
    }

    async fn get_members(&self) -> MetaResult<Vec<ElectionMember>> {
        Ok(vec![self.self_member()])
    }

    async fn is_leader(&self) -> bool {
        true
    }
}
