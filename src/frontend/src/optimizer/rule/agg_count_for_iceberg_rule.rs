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

use super::{BoxedRule, Rule};
use crate::optimizer::{plan_node::{BatchIcebergScan, LogicalIcebergScan, PlanAggCall}, PlanRef};

pub struct AggCountForIcebergRule {}
impl Rule for AggCountForIcebergRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let agg = plan.as_batch_simple_agg()?;
        if agg.core.group_key.is_empty() && agg.agg_calls().len() == 1 && agg.agg_calls()[0].eq(&PlanAggCall::count_star()){
            let mut source = agg.core.input.as_batch_iceberg_scan()?.core.clone();
            source.is_iceberg_count = true;
            println!("plan213321{:?}",source);
            return Some(BatchIcebergScan::new(source).into())
        }
        None
    }
}

impl AggCountForIcebergRule {
    pub fn create() -> BoxedRule {
        Box::new(AggCountForIcebergRule {})
    }
}