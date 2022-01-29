use risingwave_pb::{data, plan};

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
