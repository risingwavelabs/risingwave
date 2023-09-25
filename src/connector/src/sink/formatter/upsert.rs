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

pub struct UpsertFormatter<KE, VE> {
    key_encoder: KE,
    val_encoder: VE,
}

impl<KE, VE> UpsertFormatter<KE, VE> {
    pub fn new(key_encoder: KE, val_encoder: VE) -> Self {
        Self {
            key_encoder,
            val_encoder,
        }
    }
}

impl<KE: RowEncoder, VE: RowEncoder> SinkFormatter for UpsertFormatter<KE, VE> {
    type K = KE::Output;
    type V = VE::Output;

    fn format_chunk(
        &self,
        chunk: &StreamChunk,
    ) -> impl Iterator<Item = Result<(Option<Self::K>, Option<Self::V>)>> {
        std::iter::from_generator(|| {
            for (op, row) in chunk.rows() {
                let event_key_object = Some(tri!(self.key_encoder.encode(row)));

                let event_object = match op {
                    Op::Insert | Op::UpdateInsert => Some(tri!(self.val_encoder.encode(row))),
                    // Empty value with a key
                    Op::Delete => None,
                    Op::UpdateDelete => {
                        // upsert semantic does not require update delete event
                        continue;
                    }
                };

                yield Ok((event_key_object, event_object))
            }
        })
    }
}
