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

use risingwave_connector::source::iceberg::{IcebergProperties, IcebergSplitEnumerator};
use risingwave_connector::source::{ConnectorProperties, SourceEnumeratorContext};

use super::{BoxedRule, Rule};
use crate::optimizer::plan_node::{LogicalIcebergScan, LogicalSource};
use crate::optimizer::PlanRef;

pub struct SourceToIcebergScanRule {}
impl Rule for SourceToIcebergScanRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let source: &LogicalSource = plan.as_logical_source()?;
        // let s = if let ConnectorProperties::Iceberg(prop) =  
        //             ConnectorProperties::extract(source.core.catalog.unwrap().with_properties.clone(), false)?{
        //                 IcebergSplitEnumerator::new_inner(*prop, SourceEnumeratorContext::dummy().into())
        //             }else{
        //                 return None;
        //             };
        // let join_columns = s.get_all_delete_columns_name();
        if source.core.is_iceberg_connector() {
            Some(LogicalIcebergScan::new(source).into())
        } else {
            None
        }
    }
}

impl SourceToIcebergScanRule {
    pub fn create() -> BoxedRule {
        Box::new(SourceToIcebergScanRule {})
    }
}
