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

use crate::barrier::BarrierManagerRef;
use crate::storage::MetaStore;

pub type SourceManagerRef<S> = Arc<SourceManager<S>>;

#[allow(dead_code)]
pub struct SourceManager<S>
where
    S: MetaStore,
{
    meta_store_ref: Arc<S>,
    barrier_manager_ref: BarrierManagerRef<S>,
}

impl<S> SourceManager<S>
where
    S: MetaStore,
{
    pub async fn new(
        meta_store_ref: Arc<S>,
        barrier_manager_ref: BarrierManagerRef<S>,
    ) -> Result<Self> {
        Ok(Self {
            meta_store_ref,
            barrier_manager_ref,
        })
    }

    pub async fn run(&self) -> Result<()> {
        // todo: fill me
        Ok(())
    }
}
