use std::collections::HashMap;

use self::enumerator::UdfSplitEnumerator;
use self::source::UdfSplitReader;
use self::split::UdfSplit;
use super::SourceProperties;

pub mod enumerator;
pub mod source;
pub mod split;

use risingwave_common::array::Op;
use risingwave_pb::catalog::PbSource;
use risingwave_pb::expr::ExprNode;
use serde_derive::Deserialize;
use serde_with::serde_as;
use strum_macros::{Display, EnumString};
use with_options::WithOptions;

pub const UDF_CONNECTOR: &str = "udf";

#[derive(Debug, Clone, PartialEq, Display, Deserialize, EnumString)]
#[strum(serialize_all = "snake_case")]
#[allow(clippy::enum_variant_names)]
pub enum OperationName {
    Insert,
    Delete,
    UpdateDelete,
    UpdateInsert,
}

impl From<OperationName> for Op {
    fn from(op: OperationName) -> Self {
        match op {
            OperationName::Insert => Op::Insert,
            OperationName::Delete => Op::Delete,
            OperationName::UpdateDelete => Op::UpdateDelete,
            OperationName::UpdateInsert => Op::UpdateInsert,
        }
    }
}

#[serde_as]
#[derive(Clone, Debug, Deserialize, WithOptions)]
pub struct UdfProperties {
    /// The field used to specify the operation to be performed, should have any of the following values (insert, delete, update_delete, update_insert)
    #[serde(rename = "operation.field")]
    pub operation_field: Option<String>,

    /// The field used to specify the operation to be performed
    #[serde(rename = "offset.field")]
    pub offset_field: Option<String>,

    pub expr: Option<ExprNode>,

    #[serde(flatten)]
    pub unknown_fields: HashMap<String, String>,
}

impl SourceProperties for UdfProperties {
    type Split = UdfSplit;
    type SplitEnumerator = UdfSplitEnumerator;
    type SplitReader = UdfSplitReader;

    const SOURCE_NAME: &'static str = UDF_CONNECTOR;

    /// Load additional info from `PbSource`. Currently only used by CDC.
    fn init_from_pb_source(&mut self, source: &PbSource) {
        if let Ok(expr) = source.get_udf_expr() {
            self.expr = Some(expr.clone());
        }
    }
}

impl crate::source::UnknownFields for UdfProperties {
    fn unknown_fields(&self) -> HashMap<String, String> {
        self.unknown_fields.clone()
    }
}
