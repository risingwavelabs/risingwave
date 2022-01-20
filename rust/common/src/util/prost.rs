use risingwave_pb::{data, plan};
use serde::Serialize;
use serde_json::{json, Value};
use serde_with::skip_serializing_none;

use crate::error::{ErrorCode, Result, RwError};

pub trait TypeUrl {
    fn type_url() -> &'static str;
}

impl TypeUrl for plan::ExchangeNode {
    fn type_url() -> &'static str {
        "type.googleapis.com/plan.ExchangeNode"
    }
}

impl TypeUrl for data::Column {
    fn type_url() -> &'static str {
        "type.googleapis.com/data.Column"
    }
}

pub trait JsonFormatter {
    fn to_json(&self) -> Result<Value>;
}

impl JsonFormatter for plan::PlanFragment {
    fn to_json(&self) -> Result<Value> {
        self.get_root().to_json()
    }
}

#[skip_serializing_none]
#[derive(Serialize)]
struct JsonPlanNode {
    body: Option<Value>,
    children: Vec<Value>,
}

impl JsonFormatter for plan::PlanNode {
    fn to_json(&self) -> Result<Value> {
        let body = match self.get_node_body() {
            plan::plan_node::NodeBody::Exchange(x) => Some(x.to_json()?),
            _ => None,
        };
        let mut children = vec![];
        for node in self.get_children() {
            children.push(node.to_json()?);
        }
        Ok(json!(JsonPlanNode { body, children }))
    }
}

impl JsonFormatter for plan::ExchangeNode {
    fn to_json(&self) -> Result<Value> {
        let mut sources = vec![];
        for src in self.get_sources() {
            sources.push(format!("{:?}", src.get_host()))
        }
        Ok(json!({ "sources": sources }))
    }
}

pub fn json_to_pretty_string(v: &Value) -> Result<String> {
    serde_json::to_string_pretty(v).map_err(|e| {
        RwError::from(ErrorCode::InternalError(format!(
            "failed to print json to string: {}",
            e
        )))
    })
}
