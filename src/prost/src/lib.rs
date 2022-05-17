#![allow(clippy::all)]

macro_rules! define_module {
    ($name:ident) => {
        define_module!($name, stringify!($name));
    };
    ($name:ident, $str:expr) => {
#[rustfmt::skip]
        pub mod $name {
            tonic::include_proto!($str);
            include!(concat!(env!("OUT_DIR"), concat!("/", $str, ".serde.rs")));
        }
    };
}
define_module!(catalog);
define_module!(common);
define_module!(data);
define_module!(ddl_service);
define_module!(expr);
define_module!(meta);
define_module!(plan_common);
define_module!(batch_plan);
define_module!(task_service);
define_module!(stream_plan);
define_module!(stream_service);
define_module!(hummock);

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct ProstFieldNotFound(pub &'static str);

#[cfg(test)]
mod tests {
    use crate::data::{data_type, DataType};
    use crate::plan_common::{DatabaseRefId, SchemaRefId};

    #[test]
    fn test_getter() {
        let schema_id: SchemaRefId = SchemaRefId {
            database_ref_id: Some(DatabaseRefId { database_id: 0 }),
            schema_id: 0,
        };
        assert_eq!(0, schema_id.get_database_ref_id().unwrap().database_id);
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
    fn test_primitive_getter() {
        let id: DatabaseRefId = DatabaseRefId::default();
        let new_id = DatabaseRefId {
            database_id: id.get_database_id(),
        };
        assert_eq!(new_id.database_id, 0);
    }
}
