// Copyright 2024 RisingWave Labs
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

use super::RowEncoder;

pub struct TextEncoder {}

impl RowEncoder for TextEncoder {
    type Output = String;

    fn schema(&self) -> &risingwave_common::catalog::Schema {
        unimplemented!()
    }

    fn col_indices(&self) -> Option<&[usize]> {
        unimplemented!()
    }

    fn encode(&self, row: impl risingwave_common::row::Row) -> crate::sink::Result<Self::Output> {
        unimplemented!()
    }

    fn encode_cols(
        &self,
        row: impl risingwave_common::row::Row,
        col_indices: impl Iterator<Item = usize>,
    ) -> crate::sink::Result<Self::Output> {
        unimplemented!()
    }
}
