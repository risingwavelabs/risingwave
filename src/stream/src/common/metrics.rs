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

use std::sync::Arc;
use risingwave_common::id::TableId;
use crate::executor::monitor::StreamingMetrics;
use crate::task::ActorId;

#[derive(Clone)]
pub struct MetricsInfo {
    pub metrics: Arc<StreamingMetrics>,
    pub table_id: String,
    pub actor_id: String,
    pub desc: String,
}

impl MetricsInfo {
    pub fn new(
        metrics: Arc<StreamingMetrics>,
        table_id: TableId,
        actor_id: ActorId,
        desc: impl Into<String>,
    ) -> Self {
        Self {
            metrics,
            table_id: table_id.to_string(),
            actor_id: actor_id.to_string(),
            desc: desc.into(),
        }
    }

    pub fn for_test() -> Self {
        Self {
            metrics: Arc::new(StreamingMetrics::unused()),
            table_id: "table_id test".to_owned(),
            actor_id: "actor_id test".to_owned(),
            desc: "desc test".to_owned(),
        }
    }
}
