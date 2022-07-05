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

use lazy_static::lazy_static;
use regex::Regex;

pub fn generate_intertable_name_with_type(
    mview_name: &String,
    table_id: u32,
    table_type: &str,
) -> String {
    format!("__INTERNAL_{}_{}_{}", mview_name, table_type, table_id)
}

pub fn valid_table_name(table_name: &str) -> bool {
    lazy_static! {
        static ref INTERNAL_TABLE_NAME: Regex = Regex::new(r"__INTERNAL_.*_\d+").unwrap();
    }
    !INTERNAL_TABLE_NAME.is_match(table_name)
}
