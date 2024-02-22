impl ::risingwave_common::types::Fields for Data {
    fn fields() -> Vec<(&'static str, ::risingwave_common::types::DataType)> {
        vec![
            ("v1", < i16 as ::risingwave_common::types::WithDataType >
            ::default_data_type()), ("v2", < std::primitive::i32 as
            ::risingwave_common::types::WithDataType > ::default_data_type()), ("v3", <
            bool as ::risingwave_common::types::WithDataType > ::default_data_type()),
            ("v4", < Serial as ::risingwave_common::types::WithDataType >
            ::default_data_type()), ("type", < i32 as
            ::risingwave_common::types::WithDataType > ::default_data_type())
        ]
    }
    fn into_owned_row(self) -> ::risingwave_common::row::OwnedRow {
        ::risingwave_common::row::OwnedRow::new(
            vec![
                ::risingwave_common::types::ToOwnedDatum::to_owned_datum(self.v1),
                ::risingwave_common::types::ToOwnedDatum::to_owned_datum(self.v2),
                ::risingwave_common::types::ToOwnedDatum::to_owned_datum(self.v3),
                ::risingwave_common::types::ToOwnedDatum::to_owned_datum(self.v4),
                ::risingwave_common::types::ToOwnedDatum::to_owned_datum(self.r#type)
            ],
        )
    }
    fn primary_key() -> &'static [usize] {
        &[1usize, 0usize]
    }
}
impl From<Data> for ::risingwave_common::types::ScalarImpl {
    fn from(v: Data) -> Self {
        ::risingwave_common::types::StructValue::new(
                vec![
                    ::risingwave_common::types::ToOwnedDatum::to_owned_datum(v.v1),
                    ::risingwave_common::types::ToOwnedDatum::to_owned_datum(v.v2),
                    ::risingwave_common::types::ToOwnedDatum::to_owned_datum(v.v3),
                    ::risingwave_common::types::ToOwnedDatum::to_owned_datum(v.v4),
                    ::risingwave_common::types::ToOwnedDatum::to_owned_datum(v.r#type)
                ],
            )
            .into()
    }
}
