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

use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;

use crate::object::object_metrics::ObjectStoreMetrics;
use crate::object::{ObjectResult, ObjectStore};

pub mod count_map;
pub mod overlapping;
pub mod range_map;

#[async_trait::async_trait]
pub trait Scheduler: Send + Sync + 'static {
    type OS: ObjectStore;
    type C;

    fn new(store: Arc<Self::OS>, metrics: Arc<ObjectStoreMetrics>, config: Self::C) -> Self;

    async fn read(&self, path: &str, range: Range<usize>) -> ObjectResult<Bytes>;
}
