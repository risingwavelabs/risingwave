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

use std::borrow::Cow;

use simd_json::{BorrowedValue, ValueAccess};

pub(crate) fn json_object_smart_get_value<'a, 'b>(
    v: &'b simd_json::BorrowedValue<'a>,
    key: Cow<'b, str>,
) -> Option<&'b BorrowedValue<'a>> {
    let obj = v.as_object()?;
    if obj.contains_key(key.as_ref()) {
        return obj.get(key.as_ref());
    }
    for (k, v) in obj {
        if k.eq_ignore_ascii_case(key.as_ref()) {
            return Some(v);
        }
    }
    None
}
