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

use super::{Field, Result, RowEncoder, RowRef, SerToBytes, SerToString};

pub struct Base64Adapter<E>(E);

impl<E> Base64Adapter<E> {
    pub fn new(inner: E) -> Self {
        Self(inner)
    }
}

impl<E: RowEncoder> RowEncoder for Base64Adapter<E> {
    type Output = String;

    fn encode(
        &self,
        row: RowRef<'_>,
        schema: &[Field],
        col_indices: impl Iterator<Item = usize>,
    ) -> Result<Self::Output> {
        Ok(base64_encode(
            &self.0.encode(row, schema, col_indices)?.ser_to_bytes()?,
        ))
    }
}

impl SerToString for String {
    fn ser_to_string(self) -> Result<String> {
        Ok(self)
    }
}

fn base64_encode(_b: &[u8]) -> String {
    todo!()
}
