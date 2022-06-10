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

use risingwave_pb::user::{GrantPrivilege, UserInfo};

use crate::user::{UserInfoVersion, UserName};

/// `UserInfoManager` is responsible for managing users.
pub struct UserInfoManager {
    version: UserInfoVersion,
    users: HashMap<UserName, UserInfo>,
}

#[allow(clippy::derivable_impls)]
impl Default for UserInfoManager {
    fn default() -> Self {
        UserInfoManager {
            version: 0,
            users: HashMap::new(),
        }
    }
}

impl UserInfoManager {
    pub fn get_user_mut(&mut self, user_name: &str) -> Option<&mut UserInfo> {
        self.users.get_mut(user_name)
    }

    pub fn get_user_by_name(&self, user_name: &str) -> Option<&UserInfo> {
        self.users.get(user_name)
    }

    pub fn create_user(&mut self, user_info: UserInfo) {
        self.users
            .try_insert(user_info.name.clone(), user_info)
            .unwrap();
    }

    pub fn drop_user(&mut self, user_name: &str) {
        self.users.remove(user_name);
    }

    pub fn update_user(&mut self, user_info: UserInfo) {
        self.users.insert(user_info.name.clone(), user_info);
    }

    pub fn authorize(&mut self, _user_name: &str, _password: &str) -> bool {
        todo!()
    }

    pub fn verify(&self, _user_name: &str, _privileges: &[GrantPrivilege]) -> bool {
        todo!()
    }

    pub fn clear(&mut self) {
        self.users.clear();
    }

    /// Get the user info cache's version.
    pub fn version(&self) -> u64 {
        self.version
    }

    /// Set the user info cache's version.
    pub fn set_version(&mut self, version: UserInfoVersion) {
        self.version = version;
    }
}
