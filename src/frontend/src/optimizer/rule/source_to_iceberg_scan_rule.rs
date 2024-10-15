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

use super::{BoxedRule, Result, Rule};
use crate::optimizer::plan_node::LogicalIcebergScan;
use crate::optimizer::PlanRef;

pub struct SourceToIcebergScanRule {}
impl Rule for SourceToIcebergScanRule {
    fn apply(&self, plan: PlanRef) -> Result<Option<PlanRef>> {
        let source = match plan.as_logical_source() {
            Some(source) => source,
            None => return Ok(None),
        };

        if source.core.is_iceberg_connector() {
            Ok(Some(LogicalIcebergScan::new(source).into()))
        } else {
            Ok(None)
        }
    }
}

impl SourceToIcebergScanRule {
    pub fn create() -> BoxedRule {
        Box::new(SourceToIcebergScanRule {})
    }
}
