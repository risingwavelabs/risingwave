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

use risingwave_pb::user::UserInfo;

use crate::model::{MetadataModel, MetadataModelResult};

/// Column family name for user info.
const USER_INFO_CF_NAME: &str = "cf/user_info";

/// `UserInfo` stores the user information.
impl MetadataModel for UserInfo {
    type KeyType = u32;
    type PbType = UserInfo;

    fn cf_name() -> String {
        USER_INFO_CF_NAME.to_string()
    }

    fn to_protobuf(&self) -> Self::PbType {
        self.clone()
    }

    fn from_protobuf(prost: Self::PbType) -> Self {
        prost
    }

    fn key(&self) -> MetadataModelResult<Self::KeyType> {
        Ok(self.id)
    }
}
