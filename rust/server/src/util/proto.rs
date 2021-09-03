use crate::error::{ErrorCode, Result};
use protobuf::Message;
use risingwave_proto::plan;
use serde_json::{json, Value};

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
        let root_json = self.get_root().to_json()?;
        Ok(json!({
            "root": root_json,
        }))
    }
}

impl JsonFormatter for plan::PlanNode {
    fn to_json(&self) -> Result<Value> {
        let body = match self.get_node_type() {
            plan::PlanNode_PlanNodeType::CREATE_TABLE => {
                let node = unpack_from_any!(self.get_body(), plan::CreateTableNode);
                format!("{:?}", node)
            }
            plan::PlanNode_PlanNodeType::INSERT_VALUE => {
                let node = unpack_from_any!(self.get_body(), plan::InsertValueNode);
                format!("{:?}", node)
            }
            _ => "".to_string(),
        };
        Ok(json!({
          "node_type": format!("{:?}", self.get_node_type()),
          "body": body,
        }))
    }
}
