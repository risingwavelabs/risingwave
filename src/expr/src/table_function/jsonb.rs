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

//! JSONB table functions.

use anyhow::anyhow;
use risingwave_common::types::JsonbRef;
use risingwave_expr_macro::function;

use super::*;

/// Expands the top-level JSON array into a set of JSON values.
#[function("jsonb_array_elements(jsonb) -> setof jsonb")]
fn jsonb_array_elements(json: JsonbRef<'_>) -> Result<impl Iterator<Item = JsonbRef<'_>>> {
    json.array_elements().map_err(|e| anyhow!(e).into())
}

/// Expands the top-level JSON array into a set of text values.
#[function("jsonb_array_elements_text(jsonb) -> setof varchar")]
fn jsonb_array_elements_text(json: JsonbRef<'_>) -> Result<impl Iterator<Item = Box<str>> + '_> {
    let elems = jsonb_array_elements(json)?;
    Ok(elems.map(|elem| elem.force_string().into()))
}

/// Returns the set of keys in the top-level JSON object.
#[function("jsonb_object_keys(jsonb) -> setof varchar")]
fn jsonb_object_keys(json: JsonbRef<'_>) -> Result<impl Iterator<Item = &str>> {
    json.object_keys().map_err(|e| anyhow!(e).into())
}
