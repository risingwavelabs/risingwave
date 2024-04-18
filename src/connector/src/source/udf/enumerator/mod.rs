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
