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

// pub struct Reschedule {}

pub struct ScaleControllerV2 {
    metadata_manager: MetadataManager,
}

pub struct RescheduleV2 {}

use std::collections::HashMap;

use mv2::FragmentId;
use risingwave_meta_model_v2 as mv2;
use risingwave_meta_model_v2::actor::Relation::Fragment;

use crate::barrier::Command::RescheduleFragment;
use crate::manager::MetadataManager;

impl ScaleControllerV2 {
    pub async fn reschedule(&self, plan: HashMap<FragmentId, usize>) {


        // self.metadata_manager.get_upstream_root_fragments()

        // let reschedules= Default::default();
        // let result = RescheduleFragment {
        //     reschedules,
        //     table_parallelism: Default::default(),
        // };

        todo!()
    }
}
