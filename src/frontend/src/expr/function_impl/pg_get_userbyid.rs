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

use risingwave_expr::{Result, capture_context, function};

use super::context::USER_INFO_READER;
use crate::user::user_service::UserInfoReader;

#[function("pg_get_userbyid(int4) -> varchar")]
fn pg_get_userbyid(oid: i32) -> Result<Option<Box<str>>> {
    pg_get_userbyid_impl_captured(oid)
}

#[capture_context(USER_INFO_READER)]
fn pg_get_userbyid_impl(reader: &UserInfoReader, oid: i32) -> Result<Option<Box<str>>> {
    Ok(reader
        .read_guard()
        .get_user_name_by_id(oid as u32)
        .map(|s| s.into_boxed_str()))
}
