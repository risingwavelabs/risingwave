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

use std::fmt::Write;

use risingwave_common::types::JsonbRef;
use risingwave_expr::function;

#[function("jsonb_access_inner(jsonb, varchar) -> jsonb")]
pub fn jsonb_object_field<'a>(v: JsonbRef<'a>, p: &str) -> Option<JsonbRef<'a>> {
    v.access_object_field(p)
}

#[function("jsonb_access_inner(jsonb, int4) -> jsonb")]
pub fn jsonb_array_element(v: JsonbRef<'_>, p: i32) -> Option<JsonbRef<'_>> {
    let idx = if p < 0 {
        let Ok(len) = v.array_len() else {
            return None;
        };
        if ((-p) as usize) > len {
            return None;
        } else {
            len - ((-p) as usize)
        }
    } else {
        p as usize
    };
    v.access_array_element(idx)
}

#[function("jsonb_access_str(jsonb, varchar) -> varchar")]
pub fn jsonb_object_field_str(v: JsonbRef<'_>, p: &str, writer: &mut impl Write) -> Option<()> {
    let jsonb = jsonb_object_field(v, p)?;
    if jsonb.is_jsonb_null() {
        return None;
    }
    jsonb.force_str(writer).unwrap();
    Some(())
}

#[function("jsonb_access_str(jsonb, int4) -> varchar")]
pub fn jsonb_array_element_str(v: JsonbRef<'_>, p: i32, writer: &mut impl Write) -> Option<()> {
    let jsonb = jsonb_array_element(v, p)?;
    if jsonb.is_jsonb_null() {
        return None;
    }
    jsonb.force_str(writer).unwrap();
    Some(())
}
