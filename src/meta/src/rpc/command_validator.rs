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

use super::ddl_controller::{DdlCommand, DdlController};
use crate::MetaResult;

impl DdlController {
    pub async fn validate_command(&self, command: &DdlCommand) -> MetaResult<()> {
        // check whether there are more than 200 actors on a worker per core
        let worker_actor_count = self.metadata_manager.worker_actor_count().await?;

        Ok(())
    }
}
