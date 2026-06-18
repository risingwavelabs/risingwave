// Copyright 2026 RisingWave Labs
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

use risingwave_common::types::JsonbVal;
use serde::{Deserialize, Serialize};

use crate::error::ConnectorResult;
use crate::source::{SplitId, SplitMetaData};

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Hash)]
pub struct RabbitmqSplit {
    pub(crate) split_id: SplitId,
    pub(crate) queues: Vec<String>,
    pub(crate) all_queues: Vec<String>,
    pub(crate) max_connections: usize,
}

impl RabbitmqSplit {
    pub fn new(
        split_id: SplitId,
        queues: Vec<String>,
        all_queues: Vec<String>,
        max_connections: usize,
    ) -> Self {
        Self {
            split_id,
            queues,
            all_queues,
            max_connections,
        }
    }

    pub fn template(queues: Vec<String>, max_connections: usize) -> Self {
        Self::new("0".into(), queues.clone(), queues, max_connections)
    }

    pub fn queues(&self) -> &[String] {
        &self.queues
    }

    pub fn all_queues(&self) -> &[String] {
        &self.all_queues
    }

    pub fn max_connections(&self) -> usize {
        self.max_connections
    }
}

impl SplitMetaData for RabbitmqSplit {
    fn id(&self) -> SplitId {
        self.split_id.clone()
    }

    fn restore_from_json(value: JsonbVal) -> ConnectorResult<Self> {
        serde_json::from_value(value.take()).map_err(Into::into)
    }

    fn encode_to_json(&self) -> JsonbVal {
        serde_json::to_value(self.clone()).unwrap().into()
    }

    fn update_offset(&mut self, _last_seen_offset: String) -> ConnectorResult<()> {
        // RabbitMQ classic queues delete messages by ack, not by replayable offsets. Offsets are
        // delivery tags used only for checkpoint-time acknowledgements, so RisingWave must not
        // persist them as restart positions.
        Ok(())
    }
}
