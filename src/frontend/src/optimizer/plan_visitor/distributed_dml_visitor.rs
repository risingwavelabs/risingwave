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

use std::rc::Rc;

use risingwave_common::session_config::RuntimeParameters;
use risingwave_connector::source::ConnectorProperties;

use super::{DefaultBehavior, Merge};
use crate::PlanRef;
use crate::catalog::source_catalog::SourceCatalog;
use crate::optimizer::plan_node::{BatchSource, LogicalSource, StreamSource};
use crate::optimizer::plan_visitor::PlanVisitor;

#[derive(Debug, Clone, Default)]
pub struct DistributedDmlVisitor {}

impl DistributedDmlVisitor {
    pub fn dml_should_run_in_distributed(plan: PlanRef) -> bool {
        if plan
            .ctx()
            .session_ctx()
            .running_sql_runtime_parameters(RuntimeParameters::batch_enable_distributed_dml)
        {
            return true;
        }
        let mut visitor = DistributedDmlVisitor {};
        visitor.visit(plan)
    }

    fn is_iceberg_source(source_catalog: &Rc<SourceCatalog>) -> bool {
        let property = ConnectorProperties::extract(source_catalog.with_properties.clone(), false);
        if let Ok(property) = property {
            matches!(property, ConnectorProperties::Iceberg(_))
        } else {
            false
        }
    }
}

impl PlanVisitor for DistributedDmlVisitor {
    type Result = bool;

    type DefaultBehavior = impl DefaultBehavior<Self::Result>;

    fn default_behavior() -> Self::DefaultBehavior {
        Merge(|a, b| a | b)
    }

    fn visit_batch_source(&mut self, batch_source: &BatchSource) -> bool {
        if let Some(source_catalog) = &batch_source.core.catalog {
            Self::is_iceberg_source(source_catalog)
        } else {
            false
        }
    }

    fn visit_logical_source(&mut self, logical_source: &LogicalSource) -> bool {
        if let Some(source_catalog) = &logical_source.core.catalog {
            Self::is_iceberg_source(source_catalog)
        } else {
            false
        }
    }

    fn visit_stream_source(&mut self, stream_source: &StreamSource) -> bool {
        if let Some(source_catalog) = &stream_source.core.catalog {
            Self::is_iceberg_source(source_catalog)
        } else {
            false
        }
    }
}
