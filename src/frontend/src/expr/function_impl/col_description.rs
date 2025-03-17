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

use std::fmt::Write;

use risingwave_expr::{ExprError, function};

#[function("col_description(varchar, int4) -> varchar")]
fn col_description(_name: &str, _col: i32, writer: &mut impl Write) -> Result<(), ExprError> {
    // TODO: Currently we don't support `COMMENT` statement, so we just return empty string.
    writer.write_str("").unwrap();

    Ok(())
}
