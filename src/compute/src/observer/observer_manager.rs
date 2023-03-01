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

use risingwave_common::system_param::local_manager::LocalSystemParamsManagerRef;
use risingwave_common_service::observer_manager::{ObserverState, SubscribeCompute};
use risingwave_pb::meta::subscribe_response::Info;
use risingwave_pb::meta::SubscribeResponse;

pub struct ComputeObserverNode {
    system_params_manager: LocalSystemParamsManagerRef,
}

impl ObserverState for ComputeObserverNode {
    type SubscribeType = SubscribeCompute;

    fn handle_notification(&mut self, resp: SubscribeResponse) {
        let Some(info) = resp.info.as_ref() else {
            return;
        };

        match info.to_owned() {
            Info::SystemParams(p) => self.system_params_manager.try_set_params(p),
            _ => {
                panic!("error type notification");
            }
        }
    }

    fn handle_initialization_notification(&mut self, _resp: SubscribeResponse) {}
}

impl ComputeObserverNode {
    pub fn new(system_params_manager: LocalSystemParamsManagerRef) -> Self {
        Self {
            system_params_manager,
        }
    }
}
