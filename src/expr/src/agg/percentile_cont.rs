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

// TODO: better name
pub trait MyScalar = Scalar + Into<f64> + Copy;

#[build_aggregate("percentile_cont(*) -> auto")]
fn build(agg: AggCall) -> Result<Box<dyn Aggregator>> {
    let fraction = agg.direct_args[0].literal().map(|x| x.as_float64().0);

    let aggregator = match agg.args.arg_types()[0] {
        DataType::Int16 => Box::new(PercentileCont::<i16>::new(
            fraction,
            agg.return_type.clone(),
        )) as _,
        DataType::Int32 => Box::new(PercentileCont::<i32>::new(
            fraction,
            agg.return_type.clone(),
        )) as _,
        DataType::Int64 => todo!(), // FIXME: seems i64 cannot Into<f64>
        DataType::Float32 => todo!(),
        DataType::Float64 => todo!(),
        _ => {
            return Err(ExprError::InvalidParam {
                name: "ORDER BY column",
                reason: "TODO".to_string(),
            })
        }
    };
    Ok(aggregator)
}

#[derive(Clone)]
pub struct PercentileCont<T: MyScalar> {
    fractions: Option<f64>,
    return_type: DataType,
    data: Vec<T>,
}

impl<T: MyScalar> EstimateSize for PercentileCont<T> {
    fn estimated_heap_size(&self) -> usize {
        // TODO
        0
    }
}

impl<T: MyScalar> PercentileCont<T> {
    pub fn new(fractions: Option<f64>, return_type: DataType) -> Self {
        Self {
            fractions,
            return_type,
            data: vec![],
        }
    }

    fn add_datum(&mut self, datum_ref: DatumRef<'_>) {
        if let Some(datum) = datum_ref.to_owned_datum() {
            self.data.push(TryInto::<T>::try_into(datum).unwrap());
        }
    }
}

#[async_trait::async_trait]
impl<T: MyScalar> Aggregator for PercentileCont<T> {
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
        if let Some(fractions) = self.fractions {
            let rn = 1.0 + (fractions * (self.data.len() - 1) as f64);
            let crn = f64::ceil(rn);
            let frn = f64::floor(rn);
            let result: ScalarImpl = if crn == frn {
                self.data[crn as usize].into()
            } else {
                ((crn - rn) * Into::<f64>::into(self.data[frn as usize])
                    + (rn - frn) * Into::<f64>::into(self.data[crn as usize]))
                .into()
            };
            builder.append(Some(result));
        }
        Ok(())
    }

    fn estimated_size(&self) -> usize {
        EstimateSize::estimated_size(self)
    }
}
