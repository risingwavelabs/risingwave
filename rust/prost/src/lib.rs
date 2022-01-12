#![allow(clippy::all)]

#[rustfmt::skip]
#[cfg(not(doctest))]
#[path = "google.protobuf.rs"]
mod google_protobuf;

pub mod google {
    pub mod protobuf {
        #[cfg(doctest)]
        pub use prost_types::Any;

        #[cfg(not(doctest))]
        pub use super::super::google_protobuf::*;
    }
}

#[rustfmt::skip]
pub mod common;
#[rustfmt::skip]
pub mod data;
#[rustfmt::skip]
pub mod expr;
#[rustfmt::skip]
pub mod meta;
#[rustfmt::skip]
pub mod plan;
#[rustfmt::skip]
pub mod task_service;
#[rustfmt::skip]
pub mod stream_plan;
#[rustfmt::skip]
pub mod stream_service;
#[rustfmt::skip]
pub mod hummock;

#[rustfmt::skip]
#[path = "common.serde.rs"]
pub mod common_serde;
#[rustfmt::skip]
#[path = "data.serde.rs"]
pub mod data_serde;
#[rustfmt::skip]
#[path = "expr.serde.rs"]
pub mod expr_serde;
#[rustfmt::skip]
#[path = "meta.serde.rs"]
pub mod meta_serde;
#[rustfmt::skip]
#[path = "plan.serde.rs"]
pub mod plan_serde;
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
#[cfg(not(doctest))]
#[path = "google.protobuf.serde.rs"]
pub mod google_protobuf_serde;

#[cfg(test)]
mod tests {
    use crate::data::{data_type, DataType};
    use crate::plan::{DatabaseRefId, SchemaRefId};

    #[test]
    fn test_getter() {
        let schema_id: SchemaRefId = SchemaRefId {
            database_ref_id: Some(DatabaseRefId { database_id: 0 }),
            schema_id: 0,
        };
        assert_eq!(0, schema_id.get_database_ref_id().database_id);
    }

    #[test]
    fn test_enum_getter() {
        let mut data_type: DataType = DataType::default();
        data_type.type_name = data_type::TypeName::Double as i32;
        assert_eq!(data_type::TypeName::Double, data_type.get_type_name());
    }

    #[test]
    fn test_primitive_getter() {
        let id: DatabaseRefId = DatabaseRefId::default();
        let new_id = DatabaseRefId {
            database_id: id.get_database_id(),
        };
        assert_eq!(new_id.database_id, 0);
    }
}
