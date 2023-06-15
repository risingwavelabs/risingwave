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

/// Computes the discrete percentile, the first value within the ordered set of aggregated argument
/// values whose position in the ordering equals or exceeds the specified fraction. The aggregated
/// argument must be of a sortable type.
///
/// ```slt
/// statement ok
/// create table t(x int, y bigint, z real, w double, v varchar);
///
/// statement ok
/// insert into t values(1,10,100,1000,'10000'),(2,20,200,2000,'20000'),(3,30,300,3000,'30000');
///
/// query R
/// select percentile_disc(0) within group (order by x) from t;
/// ----
/// 1
///
/// query R
/// select percentile_disc(0.33) within group (order by y) from t;
/// ----
/// 10
///
/// query R
/// select percentile_disc(0.34) within group (order by z) from t;
/// ----
/// 200
///
/// query R
/// select percentile_disc(0.67) within group (order by w) from t
/// ----
/// 3000
///
/// query R
/// select percentile_disc(1) within group (order by v) from t;
/// ----
/// 30000
///
/// query R
/// select percentile_disc(NULL) within group (order by w) from t;
/// ----
/// NULL
///
/// statement ok
/// drop table t;
/// ```
#[build_aggregate("percentile_disc(*) -> *")]
fn build(agg: AggCall) -> Result<Box<dyn Aggregator>> {
    let fraction: Option<f64> = agg.direct_args[0]
        .literal()
        .map(|x| (*x.as_float64()).into());
    Ok(Box::new(PercentileDisc::new(fraction, agg.return_type)))
}

#[derive(Clone)]
pub struct PercentileDisc {
    fractions: Option<f64>,
    return_type: DataType,
    data: Vec<ScalarImpl>,
}

impl EstimateSize for PercentileDisc {
    fn estimated_heap_size(&self) -> usize {
        self.data
            .iter()
            .fold(0, |acc, x| acc + x.estimated_heap_size())
    }
}

impl PercentileDisc {
    pub fn new(fractions: Option<f64>, return_type: DataType) -> Self {
        Self {
            fractions,
            return_type,
            data: vec![],
        }
    }

    fn add_datum(&mut self, datum_ref: DatumRef<'_>) {
        if let Some(datum) = datum_ref.to_owned_datum() {
            self.data.push(datum);
        }
    }
}

#[async_trait::async_trait]
impl Aggregator for PercentileDisc {
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
        if let Some(fractions) = self.fractions && !self.data.is_empty() {
            let rn = fractions * self.data.len() as f64;
            if fractions == 0.0 {
                builder.append(Some(self.data[0].clone()));
            } else {
                builder.append(Some(self.data[f64::ceil(rn) as usize - 1].clone()));
            }
        } else {
            builder.append(Datum::None);
        }
        Ok(())
    }

    fn estimated_size(&self) -> usize {
        EstimateSize::estimated_size(self)
    }
}
