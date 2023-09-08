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

use risingwave_common::catalog::Schema;

use super::{Op, Result, RowRef, SinkFormatter};
use crate::sink::encoder::RowEncoder;

pub struct AppendOnlyFormatter<'a, KE, VE> {
    key_encoder: KE,
    val_encoder: VE,
    schema: &'a Schema,
    pk_indices: &'a [usize],
}

impl<'a, KE, VE> AppendOnlyFormatter<'a, KE, VE> {
    pub fn new(
        key_encoder: KE,
        val_encoder: VE,
        schema: &'a Schema,
        pk_indices: &'a [usize],
    ) -> Self {
        Self {
            key_encoder,
            val_encoder,
            schema,
            pk_indices,
        }
    }
}

impl<'a, KE: RowEncoder, VE: RowEncoder> SinkFormatter for AppendOnlyFormatter<'a, KE, VE> {
    type K = Option<KE::Output>;
    type V = Option<VE::Output>;

    fn format_row(&mut self, op: Op, row: RowRef<'_>) -> Result<Option<(Self::K, Self::V)>> {
        if op != Op::Insert {
            return Ok(None);
        }
        let event_key_object = Some(self.key_encoder.encode(
            row,
            &self.schema.fields,
            self.pk_indices.iter().copied(),
        )?);
        let event_object = Some(self.val_encoder.encode_all(row, &self.schema.fields)?);

        Ok(Some((event_key_object, event_object)))
    }
}
