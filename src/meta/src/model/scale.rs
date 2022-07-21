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

use std::collections::HashMap;
use std::time::SystemTime;

use risingwave_common::error::Result;
use risingwave_pb::common::HostAddress;
use risingwave_pb::meta::scale_task::TaskType;
use risingwave_pb::meta::{ScaleTask as ProstScaleTask, TaskStatus};
use uuid::Uuid;

use crate::model::MetadataModel;
use crate::storage::MetaStore;

pub type ScaleTaskId = String;

/// Column family name for scale task.
const SCALE_TASK_CF_NAME: &str = "cf/scale_task";

/// `ScaleTask` represents a scale task.
#[derive(Clone, Debug)]
pub struct ScaleTask {
    pub(crate) task_id: String,
    pub(crate) task_status: TaskStatus,
    pub(crate) task_type: TaskType,
    pub(crate) task_create_time: u64,
    pub(crate) task_end_time: u64,
    pub(crate) hosts: Vec<HostAddress>,
    pub(crate) fragment_parallelism: HashMap<u32, u32>,
}

impl MetadataModel for ScaleTask {
    type KeyType = ScaleTaskId;
    type ProstType = ProstScaleTask;

    fn cf_name() -> String {
        SCALE_TASK_CF_NAME.to_string()
    }

    fn to_protobuf(&self) -> Self::ProstType {
        Self::ProstType {
            task_id: self.task_id.clone(),
            task_status: self.task_status as i32,
            task_type: self.task_type as i32,
            task_create_time: self.task_create_time,
            task_end_time: self.task_end_time,
            hosts: self.hosts.clone(),
            fragment_parallelism: self.fragment_parallelism.clone(),
        }
    }

    fn from_protobuf(prost: Self::ProstType) -> Self {
        Self {
            task_status: prost.get_task_status().unwrap(),
            task_type: prost.get_task_type().unwrap(),
            task_id: prost.task_id,
            task_create_time: prost.task_create_time,
            task_end_time: prost.task_end_time,
            hosts: prost.hosts,
            fragment_parallelism: prost.fragment_parallelism,
        }
    }

    fn key(&self) -> Result<Self::KeyType> {
        Ok(self.task_id.clone())
    }
}

fn get_timestamp_now() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

impl ScaleTask {
    pub async fn init<S: MetaStore>(&mut self, meta_store: &S) -> Result<()> {
        self.task_id = Uuid::new_v4().to_string();
        self.task_status = TaskStatus::Pending;
        self.task_create_time = get_timestamp_now();
        self.insert(meta_store).await?;
        Ok(())
    }

    pub async fn end<S: MetaStore>(
        &mut self,
        meta_store: &S,
        end_status: TaskStatus,
    ) -> Result<()> {
        self.task_status = end_status;
        self.task_end_time = get_timestamp_now();
        self.insert(meta_store).await?;
        Ok(())
    }

    pub async fn status<S: MetaStore>(
        &mut self,
        meta_store: &S,
        task_status: TaskStatus,
    ) -> Result<()> {
        self.task_status = task_status;
        self.insert(meta_store).await?;
        Ok(())
    }
}
