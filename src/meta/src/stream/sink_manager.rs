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

use std::sync::Arc;

use risingwave_common::error::Result;
use risingwave_pb::catalog::Sink;
use tokio::sync::Mutex;

use crate::manager::{MetaSrvEnv, SinkId};
use crate::storage::MetaStore;
use crate::stream::FragmentManagerRef;

pub type SinkManagerRef<S> = Arc<SinkManager<S>>;

#[allow(dead_code)]
pub struct SinkManager<S: MetaStore> {
    env: MetaSrvEnv<S>,
    core: Arc<Mutex<SinkManagerCore<S>>>,
}

pub struct SinkManagerCore<S: MetaStore> {
    pub fragment_manager: FragmentManagerRef<S>,
}

impl<S> SinkManagerCore<S>
where
    S: MetaStore,
{
    fn new(fragment_manager: FragmentManagerRef<S>) -> Self {
        Self { fragment_manager }
    }
}

impl<S> SinkManager<S>
where
    S: MetaStore,
{
    pub async fn new(env: MetaSrvEnv<S>, fragment_manager: FragmentManagerRef<S>) -> Result<Self> {
        let core = Arc::new(Mutex::new(SinkManagerCore::new(fragment_manager)));

        Ok(Self { env, core })
    }

    /// Broadcast the create sink request to all compute nodes.
    pub async fn create_sink(&self, sink: &Sink) -> Result<()> {
        todo!();
    }

    pub async fn drop_sink(&self, sink_id: SinkId) -> Result<()> {
        todo!();
    }

    async fn tick(&self) -> Result<()> {
        Ok(())
    }

    pub async fn run(&self) -> Result<()> {
        todo!();
    }
}
