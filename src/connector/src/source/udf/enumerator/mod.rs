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
use std::sync::Arc;

use anyhow::Context;
use async_trait::async_trait;
use risingwave_pb::expr::ExprNode;

use super::split::UdfSplit;
use super::UdfProperties;
use crate::error::ConnectorResult;
use crate::source::{SourceEnumeratorContextRef, SplitEnumerator};

pub struct UdfSplitEnumerator {
    expr: ExprNode,
}

#[async_trait]
impl SplitEnumerator for UdfSplitEnumerator {
    type Properties = UdfProperties;
    type Split = UdfSplit;

    async fn new(
        properties: Self::Properties,
        _context: SourceEnumeratorContextRef,
    ) -> ConnectorResult<UdfSplitEnumerator> {
        Ok(Self {
            expr: properties.expr.context("expr is required")?,
        })
    }

    async fn list_splits(&mut self) -> ConnectorResult<Vec<UdfSplit>> {
        let udf_split = UdfSplit {
            split_id: Arc::from("0"),
            expr: self.expr.clone(),
        };

        Ok(vec![udf_split])
    }
}
