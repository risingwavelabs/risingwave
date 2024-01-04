impl ::risingwave_common::types::Fields for Data {
    fn fields() -> Vec<(&'static str, ::risingwave_common::types::DataType)> {
        vec![
            ("v1", < i16 as ::risingwave_common::types::WithDataType >
            ::default_data_type()), ("v2", < std::primitive::i32 as
            ::risingwave_common::types::WithDataType > ::default_data_type()), ("v3", <
            bool as ::risingwave_common::types::WithDataType > ::default_data_type()),
            ("v4", < Serial as ::risingwave_common::types::WithDataType >
            ::default_data_type())
        ]
    }
}
