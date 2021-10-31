use crate::error::{ErrorCode, Result, RwError};
use protobuf::Message;
use risingwave_proto::plan;
use risingwave_proto::task_service;
use serde::Serialize;
use serde_json::{json, Value};
use serde_with::skip_serializing_none;

#[macro_export]
macro_rules! unpack_from_any {
    ($source:expr, $node_type:ty) => {
        <$node_type>::parse_from_bytes($source.get_value()).map_err(ErrorCode::ProtobufError)?
    };
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
    node_type: String,
    body: Option<Value>,
    children: Vec<Value>,
}

impl JsonFormatter for plan::PlanNode {
    fn to_json(&self) -> Result<Value> {
        let body = match self.get_node_type() {
            plan::PlanNode_PlanNodeType::EXCHANGE => {
                Some(unpack_from_any!(self.get_body(), task_service::ExchangeNode).to_json()?)
            }
            _ => None,
        };
        let mut children = vec![];
        for node in self.get_children() {
            children.push(node.to_json()?);
        }
        Ok(json!(JsonPlanNode {
            node_type: format!("{:?}", self.get_node_type()),
            body,
            children
        }))
    }
}

impl JsonFormatter for task_service::ExchangeNode {
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
