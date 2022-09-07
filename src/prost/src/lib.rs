// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![allow(clippy::all)]
#![allow(rustdoc::bare_urls)]

#[rustfmt::skip]
#[cfg_attr(madsim, path = "sim/catalog.rs")]
pub mod catalog;
#[rustfmt::skip]
#[cfg_attr(madsim, path = "sim/common.rs")]
pub mod common;
#[rustfmt::skip]
#[cfg_attr(madsim, path = "sim/data.rs")]
pub mod data;
#[rustfmt::skip]
#[cfg_attr(madsim, path = "sim/ddl_service.rs")]
pub mod ddl_service;
#[rustfmt::skip]
#[cfg_attr(madsim, path = "sim/expr.rs")]
pub mod expr;
#[rustfmt::skip]
#[cfg_attr(madsim, path = "sim/meta.rs")]
pub mod meta;
#[rustfmt::skip]
#[cfg_attr(madsim, path = "sim/plan_common.rs")]
pub mod plan_common;
#[rustfmt::skip]
#[cfg_attr(madsim, path = "sim/batch_plan.rs")]
pub mod batch_plan;
#[rustfmt::skip]
#[cfg_attr(madsim, path = "sim/task_service.rs")]
pub mod task_service;
#[rustfmt::skip]
#[cfg_attr(madsim, path = "sim/stream_plan.rs")]
pub mod stream_plan;
#[rustfmt::skip]
#[cfg_attr(madsim, path = "sim/stream_service.rs")]
pub mod stream_service;
#[rustfmt::skip]
#[cfg_attr(madsim, path = "sim/hummock.rs")]
pub mod hummock;
#[rustfmt::skip]
#[cfg_attr(madsim, path = "sim/user.rs")]
pub mod user;
#[rustfmt::skip]
#[cfg_attr(madsim, path = "sim/source.rs")]
pub mod source;
#[rustfmt::skip]
#[cfg_attr(madsim, path = "sim/monitor_service.rs")]
pub mod monitor_service;
#[rustfmt::skip]
#[cfg_attr(madsim, path = "sim/health.rs")]
pub mod health;

#[rustfmt::skip]
#[path = "catalog.serde.rs"]
pub mod catalog_serde;
#[rustfmt::skip]
#[path = "common.serde.rs"]
pub mod common_serde;
#[rustfmt::skip]
#[path = "data.serde.rs"]
pub mod data_serde;
#[rustfmt::skip]
#[path = "ddl_service.serde.rs"]
pub mod ddl_service_serde;
#[rustfmt::skip]
#[path = "expr.serde.rs"]
pub mod expr_serde;
#[rustfmt::skip]
#[path = "meta.serde.rs"]
pub mod meta_serde;
#[rustfmt::skip]
#[path = "plan_common.serde.rs"]
pub mod plan_common_serde;
#[rustfmt::skip]
#[path = "batch_plan.serde.rs"]
pub mod batch_plan_serde;
#[rustfmt::skip]
#[path = "task_service.serde.rs"]
pub mod task_service_serde;
#[rustfmt::skip]
#[path = "stream_plan.serde.rs"]
pub mod stream_plan_serde;
#[rustfmt::skip]
#[path = "stream_service.serde.rs"]
pub mod stream_service_serde;
#[rustfmt::skip]
#[path = "hummock.serde.rs"]
pub mod hummock_serde;
#[rustfmt::skip]
#[path = "user.serde.rs"]
pub mod user_serde;
#[rustfmt::skip]
#[path = "source.serde.rs"]
pub mod source_serde;
#[rustfmt::skip]
#[path = "monitor_service.serde.rs"]
pub mod monitor_service_serde;


#[derive(Clone, PartialEq, Eq, Debug)]
pub struct ProstFieldNotFound(pub &'static str);

impl From<ProstFieldNotFound> for tonic::Status {
    fn from(e: ProstFieldNotFound) -> Self {
        tonic::Status::new(tonic::Code::Internal, e.0)
    }
}

#[cfg(test)]
mod tests {
    use crate::data::{data_type, DataType};
    use crate::plan_common::Field;

    #[test]
    fn test_getter() {
        let mut data_type: DataType = DataType::default();
        data_type.is_nullable = true;
        let field = Field {
            data_type: Some(data_type),
            name: "".to_string(),
        };
        assert!(field.get_data_type().unwrap().is_nullable);
    }

    #[test]
    fn test_enum_getter() {
        let mut data_type: DataType = DataType::default();
        data_type.type_name = data_type::TypeName::Double as i32;
        assert_eq!(
            data_type::TypeName::Double,
            data_type.get_type_name().unwrap()
        );
    }

    #[test]
    fn test_enum_unspecified() {
        let mut data_type: DataType = DataType::default();
        data_type.type_name = data_type::TypeName::TypeUnspecified as i32;
        assert!(data_type.get_type_name().is_err());
    }

    #[test]
    fn test_primitive_getter() {
        let data_type: DataType = DataType::default();
        let new_data_type = DataType {
            is_nullable: data_type.get_is_nullable(),
            ..Default::default()
        };
        assert!(!new_data_type.is_nullable);
    }
}
