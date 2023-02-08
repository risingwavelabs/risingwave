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

use risingwave_common::types::DataType;

use crate::Result;

#[inline(always)]
pub fn format_type(oid: i32, _typemod: i32, writer: &mut dyn Write) -> Result<()> {
    writer
        .write_str(
            &DataType::from_oid(oid)
                .map(|t| t.to_string())
                .unwrap_or("???".to_string()),
        )
        .unwrap();
    Ok(())
}
