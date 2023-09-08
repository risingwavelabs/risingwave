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

use prost::Message;
use prost_reflect::DynamicMessage;

use super::{Field, Result, RowEncoder, RowRef, SerToBytes};

#[derive(Default)]
pub struct ProtoEncoder {
    // desc: MessageDescriptor,
}

impl RowEncoder for ProtoEncoder {
    type Output = DynamicMessage;

    fn encode(
        &self,
        _row: RowRef<'_>,
        _schema: &[Field],
        _col_indices: impl Iterator<Item = usize>,
    ) -> Result<Self::Output> {
        todo!()
    }
}

impl SerToBytes for DynamicMessage {
    fn ser_to_bytes(&self) -> Result<Vec<u8>> {
        Ok(self.encode_to_vec())
    }
}
