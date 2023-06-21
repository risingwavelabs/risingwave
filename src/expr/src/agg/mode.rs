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

use risingwave_common::array::*;
use risingwave_common::estimate_size::EstimateSize;
use risingwave_common::types::*;
use risingwave_expr_macro::build_aggregate;

use super::Aggregator;
use crate::agg::AggCall;
use crate::Result;

#[build_aggregate("mode(*) -> auto")]
fn build(agg: AggCall) -> Result<Box<dyn Aggregator>> {
    Ok(Box::new(Mode::new(agg.return_type)))
}

/// Computes the mode, the most frequent value of the aggregated argument (arbitrarily choosing the
/// first one if there are multiple equally-frequent values). The aggregated argument must be of a
/// sortable type.
///
/// ```slt
/// query I
/// select mode() within group (order by unnest) from unnest(array[1]);
/// ----
/// 1
///
/// query I
/// select mode() within group (order by unnest) from unnest(array[1,2,2,3,3,4,4,4]);
/// ----
/// 4
///
/// query R
/// select mode() within group (order by unnest) from unnest(array[0.1,0.2,0.2,0.4,0.4,0.3,0.3,0.4]);
/// ----
/// 0.4
///
/// query R
/// select mode() within group (order by unnest) from unnest(array[1,2,2,3,3,4,4,4,3]);
/// ----
/// 3
///
/// query T
/// select mode() within group (order by unnest) from unnest(array['1','2','2','3','3','4','4','4','3']);
/// ----
/// 3
///
/// query I
/// select mode() within group (order by unnest) from unnest(array[]::int[]);
/// ----
/// NULL
/// ```
#[derive(Clone, EstimateSize)]
pub struct Mode {
    return_type: DataType,
    cur_mode: Datum,
    cur_mode_freq: usize,
    cur_item: Datum,
    cur_item_freq: usize,
}

impl Mode {
    pub fn new(return_type: DataType) -> Self {
        Self {
            return_type,
            cur_mode: None,
            cur_mode_freq: 0,
            cur_item: None,
            cur_item_freq: 0,
        }
    }

    fn add_datum(&mut self, datum_ref: DatumRef<'_>) {
        let datum = datum_ref.to_owned_datum();
        if datum.is_some() && self.cur_item == datum {
            self.cur_item_freq += 1;
        } else if datum.is_some() {
            self.cur_item = datum;
            self.cur_item_freq = 1;
        }
        if self.cur_item_freq > self.cur_mode_freq {
            self.cur_mode = self.cur_item.clone();
            self.cur_mode_freq = self.cur_item_freq;
        }
    }
}

#[async_trait::async_trait]
impl Aggregator for Mode {
    fn return_type(&self) -> DataType {
        self.return_type.clone()
    }

    async fn update_multi(
        &mut self,
        input: &DataChunk,
        start_row_id: usize,
        end_row_id: usize,
    ) -> Result<()> {
        let array = input.column_at(0);
        for row_id in start_row_id..end_row_id {
            self.add_datum(array.value_at(row_id));
        }
        Ok(())
    }

    fn output(&mut self, builder: &mut ArrayBuilderImpl) -> Result<()> {
        builder.append(self.cur_mode.clone());
        Ok(())
    }

    fn estimated_size(&self) -> usize {
        EstimateSize::estimated_size(self)
    }
}
