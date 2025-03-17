// Copyright 2025 RisingWave Labs
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
use crate::optimizer::PlanRef;
use crate::optimizer::plan_node::{LogicalKafkaScan, LogicalSource};

pub struct SourceToKafkaScanRule {}
impl Rule for SourceToKafkaScanRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let source: &LogicalSource = plan.as_logical_source()?;
        if source.core.is_kafka_connector() {
            Some(LogicalKafkaScan::create(source))
        } else {
            None
        }
    }
}

impl SourceToKafkaScanRule {
    pub fn create() -> BoxedRule {
        Box::new(SourceToKafkaScanRule {})
    }
}
