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
