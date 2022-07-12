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

use risingwave_common::error::Result;
use risingwave_pb::meta::ScaleTask;

use crate::model::MetadataModel;

pub type ScaleTaskId = u32;

/// Column family name for scale task.
const SCALE_TASK_CF_NAME: &str = "cf/scale_task";

/// `ScaleTask` represents a scale task.
impl MetadataModel for ScaleTask {
    type KeyType = ScaleTaskId;
    type ProstType = ScaleTask;

    fn cf_name() -> String {
        SCALE_TASK_CF_NAME.to_string()
    }

    fn to_protobuf(&self) -> Self::ProstType {
        self.clone()
    }

    fn from_protobuf(prost: Self::ProstType) -> Self {
        prost
    }

    fn key(&self) -> Result<Self::KeyType> {
        Ok(self.task_id)
    }
}
