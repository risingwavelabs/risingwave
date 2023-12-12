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

use risingwave_common::array::Op;

use super::{Result, SinkFormatter, StreamChunk};
use crate::sink::encoder::RowEncoder;
use crate::tri;

pub struct AppendOnlyFormatter<KE, VE> {
    key_encoder: Option<KE>,
    val_encoder: VE,
}

impl<KE, VE> AppendOnlyFormatter<KE, VE> {
    pub fn new(key_encoder: Option<KE>, val_encoder: VE) -> Self {
        Self {
            key_encoder,
            val_encoder,
        }
    }
}

impl<KE: RowEncoder, VE: RowEncoder> SinkFormatter for AppendOnlyFormatter<KE, VE> {
    type K = KE::Output;
    type V = VE::Output;

    fn format_chunk(
        &self,
        chunk: &StreamChunk,
    ) -> impl Iterator<Item = Result<(Option<Self::K>, Option<Self::V>)>> {
        std::iter::from_coroutine(|| {
            for (op, row) in chunk.rows() {
                if op != Op::Insert {
                    continue;
                }
                let event_key_object = match &self.key_encoder {
                    Some(key_encoder) => Some(tri!(key_encoder.encode(row))),
                    None => None,
                };
                let event_object = Some(tri!(self.val_encoder.encode(row)));

                yield Ok((event_key_object, event_object))
            }
        })
    }
}
