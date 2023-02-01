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

use itertools::Itertools;
use risingwave_pb::user::{GrantPrivilege, UserInfo};

use crate::user::{UserId, UserInfoVersion};

/// `UserInfoManager` is responsible for managing users.
pub struct UserInfoManager {
    version: UserInfoVersion,
    user_by_name: HashMap<String, UserInfo>,
    user_name_by_id: HashMap<UserId, String>,
}

#[expect(clippy::derivable_impls)]
impl Default for UserInfoManager {
    fn default() -> Self {
        UserInfoManager {
            version: 0,
            user_by_name: HashMap::new(),
            user_name_by_id: HashMap::new(),
        }
    }
}

impl UserInfoManager {
    pub fn get_user_mut(&mut self, id: UserId) -> Option<&mut UserInfo> {
        let name = self.user_name_by_id.get(&id)?;
        self.user_by_name.get_mut(name)
    }

    pub fn get_all_users(&self) -> Vec<UserInfo> {
        self.user_by_name.values().cloned().collect_vec()
    }

    pub fn get_user_by_name(&self, user_name: &str) -> Option<&UserInfo> {
        self.user_by_name.get(user_name)
    }

    pub fn get_user_name_by_id(&self, id: UserId) -> Option<String> {
        self.user_name_by_id.get(&id).cloned()
    }

    pub fn get_user_name_map(&self) -> &HashMap<UserId, String> {
        &self.user_name_by_id
    }

    pub fn create_user(&mut self, user_info: UserInfo) {
        let id = user_info.id;
        let name = user_info.name.clone();
        self.user_by_name
            .try_insert(name.clone(), user_info)
            .unwrap();
        self.user_name_by_id.try_insert(id, name).unwrap();
    }

    pub fn drop_user(&mut self, id: UserId) {
        let name = self.user_name_by_id.remove(&id).unwrap();
        self.user_by_name.remove(&name).unwrap();
    }

    pub fn update_user(&mut self, user_info: UserInfo) {
        let id = user_info.id;
        let name = user_info.name.clone();
        if let Some(old_name) = self.get_user_name_by_id(id) {
            self.user_by_name.remove(&old_name);
            self.user_by_name.insert(name.clone(), user_info);
        } else {
            self.user_by_name.insert(name.clone(), user_info).unwrap();
        }
        self.user_name_by_id.insert(id, name).unwrap();
    }

    pub fn authorize(&mut self, _user_name: &str, _password: &str) -> bool {
        todo!()
    }

    pub fn verify(&self, _user_name: &str, _privileges: &[GrantPrivilege]) -> bool {
        todo!()
    }

    pub fn clear(&mut self) {
        self.user_by_name.clear();
        self.user_name_by_id.clear();
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
