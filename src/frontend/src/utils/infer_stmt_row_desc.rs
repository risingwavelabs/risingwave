// Copyright 2023 RisingWave Labs
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

use pgwire::pg_field_descriptor::PgFieldDescriptor;
use risingwave_common::types::DataType;
use risingwave_sqlparser::ast::ShowObject;

/// `infer_stmt_row_desc` is used to infer the row description for different show objects.
pub fn infer_show_object(objects: &ShowObject) -> Vec<PgFieldDescriptor> {
    match objects {
        ShowObject::Columns { .. } => vec![
            PgFieldDescriptor::new(
                "Name".to_owned(),
                DataType::Varchar.to_oid(),
                DataType::Varchar.type_len(),
            ),
            PgFieldDescriptor::new(
                "Type".to_owned(),
                DataType::Varchar.to_oid(),
                DataType::Varchar.type_len(),
            ),
            PgFieldDescriptor::new(
                "Is Hidden".to_owned(),
                DataType::Varchar.to_oid(),
                DataType::Varchar.type_len(),
            ),
        ],
        ShowObject::Connection { .. } => vec![
            PgFieldDescriptor::new(
                "Name".to_owned(),
                DataType::Varchar.to_oid(),
                DataType::Varchar.type_len(),
            ),
            PgFieldDescriptor::new(
                "Type".to_owned(),
                DataType::Varchar.to_oid(),
                DataType::Varchar.type_len(),
            ),
            PgFieldDescriptor::new(
                "Properties".to_owned(),
                DataType::Varchar.to_oid(),
                DataType::Varchar.type_len(),
            ),
        ],
        ShowObject::Function { .. } => vec![
            PgFieldDescriptor::new(
                "Name".to_owned(),
                DataType::Varchar.to_oid(),
                DataType::Varchar.type_len(),
            ),
            PgFieldDescriptor::new(
                "Arguments".to_owned(),
                DataType::Varchar.to_oid(),
                DataType::Varchar.type_len(),
            ),
            PgFieldDescriptor::new(
                "Return Type".to_owned(),
                DataType::Varchar.to_oid(),
                DataType::Varchar.type_len(),
            ),
            PgFieldDescriptor::new(
                "Language".to_owned(),
                DataType::Varchar.to_oid(),
                DataType::Varchar.type_len(),
            ),
            PgFieldDescriptor::new(
                "Link".to_owned(),
                DataType::Varchar.to_oid(),
                DataType::Varchar.type_len(),
            ),
        ],
        ShowObject::Indexes { .. } => vec![
            PgFieldDescriptor::new(
                "Name".to_owned(),
                DataType::Varchar.to_oid(),
                DataType::Varchar.type_len(),
            ),
            PgFieldDescriptor::new(
                "On".to_owned(),
                DataType::Varchar.to_oid(),
                DataType::Varchar.type_len(),
            ),
            PgFieldDescriptor::new(
                "Key".to_owned(),
                DataType::Varchar.to_oid(),
                DataType::Varchar.type_len(),
            ),
            PgFieldDescriptor::new(
                "Include".to_owned(),
                DataType::Varchar.to_oid(),
                DataType::Varchar.type_len(),
            ),
            PgFieldDescriptor::new(
                "Distributed By".to_owned(),
                DataType::Varchar.to_oid(),
                DataType::Varchar.type_len(),
            ),
        ],
        ShowObject::Cluster => vec![
            PgFieldDescriptor::new(
                "Addr".to_owned(),
                DataType::Varchar.to_oid(),
                DataType::Varchar.type_len(),
            ),
            PgFieldDescriptor::new(
                "State".to_owned(),
                DataType::Varchar.to_oid(),
                DataType::Varchar.type_len(),
            ),
            PgFieldDescriptor::new(
                "Parallel Units".to_owned(),
                DataType::Varchar.to_oid(),
                DataType::Varchar.type_len(),
            ),
            PgFieldDescriptor::new(
                "Is Streaming".to_owned(),
                DataType::Varchar.to_oid(),
                DataType::Varchar.type_len(),
            ),
            PgFieldDescriptor::new(
                "Is Serving".to_owned(),
                DataType::Varchar.to_oid(),
                DataType::Varchar.type_len(),
            ),
            PgFieldDescriptor::new(
                "Is Unschedulable".to_owned(),
                DataType::Varchar.to_oid(),
                DataType::Varchar.type_len(),
            ),
        ],
        ShowObject::Jobs => vec![
            PgFieldDescriptor::new(
                "Id".to_owned(),
                DataType::Int64.to_oid(),
                DataType::Int64.type_len(),
            ),
            PgFieldDescriptor::new(
                "Statement".to_owned(),
                DataType::Varchar.to_oid(),
                DataType::Varchar.type_len(),
            ),
            PgFieldDescriptor::new(
                "Progress".to_owned(),
                DataType::Varchar.to_oid(),
                DataType::Varchar.type_len(),
            ),
        ],
        _ => vec![PgFieldDescriptor::new(
            "Name".to_owned(),
            DataType::Varchar.to_oid(),
            DataType::Varchar.type_len(),
        )],
    }
}
