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

use risingwave_common::util::column_index_mapping::ColIndexMapping;

use super::super::plan_node::*;
use super::{BoxedRule, Rule};

/// Add an identity project to avoid parent exchange connecting directly to the share operator.
pub struct AvoidExchangeShareRule {}

impl Rule for AvoidExchangeShareRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let stream_exchange: &StreamExchange = plan.as_stream_exchange()?;
        let input = stream_exchange.input();
        let stream_share: &StreamShare = input.as_stream_share()?;

        // Remember to keep the DAG intact.
        let identity = ColIndexMapping::identity(stream_share.schema().len());
        let logical_project = generic::Project::with_mapping(input, identity);
        let stream_project = StreamProject::new(logical_project);

        Some(
            stream_exchange
                .clone_with_input(stream_project.into())
                .into(),
        )
    }
}

impl AvoidExchangeShareRule {
    pub fn create() -> BoxedRule {
        Box::new(Self {})
    }
}
