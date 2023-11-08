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

use simd_json::prelude::ValueAsContainer;
use simd_json::BorrowedValue;

/// Get a value from a json object by key, case insensitive.
///
/// Returns `None` if the given json value is not an object, or the key is not found.
pub(crate) fn json_object_get_case_insensitive<'a, 'b>(
    v: &'b simd_json::BorrowedValue<'a>,
    key: &'b str,
) -> Option<&'b BorrowedValue<'a>> {
    let obj = v.as_object()?;
    let value = obj.get(key);
    if value.is_some() {
        return value; // fast path
    }
    for (k, v) in obj {
        if k.eq_ignore_ascii_case(key) {
            return Some(v);
        }
    }
    None
}
