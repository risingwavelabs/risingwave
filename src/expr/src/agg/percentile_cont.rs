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
use crate::{ExprError, Result};

#[build_aggregate("percentile_cont(float64) -> float64")]
fn build(agg: AggCall) -> Result<Box<dyn Aggregator>> {
    let fraction: Option<f64> = if let Some(literal) = agg.direct_args[0].literal() {
        let arg = literal.as_float64().clone().into();
        if arg > 1.0 || arg < 0.0 {
            return Err(ExprError::InvalidParam {
                name: "fraction",
                reason: "must between 0 and 1".to_string(),
            });
        }
        Some(arg)
    } else {
        None
    };

    Ok(Box::new(PercentileCont::new(fraction)))
}

#[derive(Clone, EstimateSize)]
pub struct PercentileCont {
    fractions: Option<f64>,
    data: Vec<f64>,
}

impl PercentileCont {
    pub fn new(fractions: Option<f64>) -> Self {
        Self {
            fractions,
            data: vec![],
        }
    }

    fn add_datum(&mut self, datum_ref: DatumRef<'_>) {
        if let Some(datum) = datum_ref.to_owned_datum() {
            self.data.push(datum.as_float64().clone().into());
        }
    }
}

#[async_trait::async_trait]
impl Aggregator for PercentileCont {
    fn return_type(&self) -> DataType {
        DataType::Float64
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
        if let Some(fractions) = self.fractions && !self.data.is_empty() {
            let rn = fractions * (self.data.len() - 1) as f64;
            let crn = f64::ceil(rn);
            let frn = f64::floor(rn);
            let result = if crn == frn {
                self.data[crn as usize]
            } else {
                (crn - rn) * self.data[frn as usize]
                    + (rn - frn) * self.data[crn as usize]
            };
            builder.append(Some(ScalarImpl::Float64(result.into())));
        } else {
            builder.append(Datum::None);
        }
        Ok(())
    }

    fn estimated_size(&self) -> usize {
        EstimateSize::estimated_size(self)
    }
}
