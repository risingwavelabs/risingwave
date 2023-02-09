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

use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use itertools::Itertools;
use parking_lot::RwLock;
use pgwire::pg_server::SessionId;
use uuid::Uuid;

use crate::catalog::{DatabaseId, SchemaId};
use crate::meta_client::FrontendMetaClient;

#[derive(Clone, Debug, Hash, Eq, PartialEq)]
pub struct TaskId {
    pub id: String,
}

impl std::fmt::Display for TaskId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "TaskId:{}", self.id)
    }
}

impl Default for TaskId {
    fn default() -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
        }
    }
}

pub type StreamingJobTrackerRef = Arc<StreamingJobTracker>;

pub struct StreamingJobTracker {
    creating_streaming_job: RwLock<HashMap<TaskId, CreatingStreamingJobInfo>>,
    meta_client: Arc<dyn FrontendMetaClient>,
}

impl StreamingJobTracker {
    pub fn new(meta_client: Arc<dyn FrontendMetaClient>) -> Self {
        Self {
            creating_streaming_job: RwLock::new(HashMap::default()),
            meta_client,
        }
    }
}

#[derive(Clone, Default)]
pub struct CreatingStreamingJobInfo {
    /// Identified by process_id, secret_key.
    session_id: SessionId,
    namespace: (DatabaseId, SchemaId),
    /// Streaming job name.
    name: String,
}

impl CreatingStreamingJobInfo {
    pub fn new(
        session_id: SessionId,
        database_id: DatabaseId,
        schema_id: SchemaId,
        name: String,
    ) -> Self {
        Self {
            session_id,
            namespace: (database_id, schema_id),
            name,
        }
    }
}

impl StreamingJobTracker {
    pub fn add_job(&self, task_id: TaskId, info: CreatingStreamingJobInfo) {
        self.creating_streaming_job.write().insert(task_id, info);
    }

    pub fn delete_job(&self, task_id: &TaskId) {
        self.creating_streaming_job.write().remove(task_id);
    }

    pub fn abort_jobs(&self, session_id: SessionId) {
        let jobs = self
            .creating_streaming_job
            .read()
            .values()
            .filter(|job| job.session_id == session_id)
            .cloned()
            .collect_vec();

        for job in jobs {
            let client = self.meta_client.clone();
            tokio::spawn(async move {
                client
                    .cancel_creating_job(job.namespace.0, job.namespace.1, &job.name)
                    .await
            });
        }
    }
}
