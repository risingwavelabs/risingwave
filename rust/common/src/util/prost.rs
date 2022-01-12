use risingwave_pb::{data, plan, task_service};
use serde::Serialize;
use serde_json::{json, Value};
use serde_with::skip_serializing_none;

use crate::error::{ErrorCode, Result, RwError};

pub trait TypeUrl {
    fn type_url() -> &'static str;
}

pub fn pack_to_any<M>(msg: &M) -> risingwave_pb::google::protobuf::Any
where
    M: prost::Message + TypeUrl,
{
    risingwave_pb::google::protobuf::Any {
        type_url: M::type_url().to_owned(),
        value: msg.encode_to_vec(),
    }
}

pub fn unpack_from_any<M>(msg: &risingwave_pb::google::protobuf::Any) -> Option<M>
where
    M: prost::Message + TypeUrl + Default,
{
    if msg.type_url == M::type_url() {
        Some(M::decode(&msg.value[..]).ok()?)
    } else {
        None
    }
}

impl TypeUrl for task_service::ExchangeNode {
    fn type_url() -> &'static str {
        "type.googleapis.com/task_service.ExchangeNode"
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
    node_type: String,
    body: Option<Value>,
    children: Vec<Value>,
}

impl JsonFormatter for plan::PlanNode {
    fn to_json(&self) -> Result<Value> {
        let body = match self.get_node_type() {
            plan::plan_node::PlanNodeType::Exchange => Some(
                unpack_from_any::<task_service::ExchangeNode>(self.get_body())
                    .unwrap()
                    .to_json()?,
            ),
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
