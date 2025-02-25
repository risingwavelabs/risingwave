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

use risingwave_common::catalog::Schema;

use crate::Binder;
use crate::error::Result;

#[derive(Debug, Clone)]
pub struct BoundFetchCursor {
    pub cursor_name: String,

    pub count: u32,

    pub returning_schema: Option<Schema>,
}

impl Binder {
    pub fn bind_fetch_cursor(
        &mut self,
        cursor_name: String,
        count: u32,
        returning_schema: Option<Schema>,
    ) -> Result<BoundFetchCursor> {
        Ok(BoundFetchCursor {
            cursor_name,
            count,
            returning_schema,
        })
    }
}
