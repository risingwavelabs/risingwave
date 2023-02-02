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

use risingwave_pb::meta::SystemParams;

use super::{MetadataModel, MetadataModelResult};

const SYSTEM_PARAM_CF_NAME: &str = "cf/system_params";
const SYSTEM_PARAM_KEY: u32 = 0;

impl MetadataModel for SystemParams {
    type KeyType = u32;
    type ProstType = SystemParams;

    fn cf_name() -> String {
        SYSTEM_PARAM_CF_NAME.to_string()
    }

    fn to_protobuf(&self) -> Self::ProstType {
        self.clone()
    }

    fn from_protobuf(prost: Self::ProstType) -> Self {
        prost
    }

    fn key(&self) -> MetadataModelResult<Self::KeyType> {
        Ok(SYSTEM_PARAM_KEY)
    }
}
