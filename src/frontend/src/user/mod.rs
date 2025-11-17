// Copyright 2025 RisingWave Labs
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

use risingwave_common::id::ObjectId;
use user_catalog::UserCatalog;

pub(crate) mod user_authentication;
pub(crate) mod user_catalog;
pub(crate) mod user_manager;
pub mod user_privilege;
pub(crate) mod user_service;

pub type UserId = u32;
pub type UserInfoVersion = u64;

/// Check if the current user has access to the object.
pub fn has_access_to_object(
    current_user: &UserCatalog,
    obj_id: impl Into<ObjectId>,
    owner_id: UserId,
) -> bool {
    let obj_id = obj_id.into();
    owner_id == current_user.id || current_user.check_object_visibility(obj_id)
}
