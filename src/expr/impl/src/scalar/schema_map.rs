use risingwave_common::types::{
    DataType, DatumCow, DatumRef, ListValue, MapValue, ScalarImpl, StructValue, ToOwnedDatum,
    data_types,
};
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_expr::expr::Context;
use risingwave_expr::function;

fn do_map<'a>(datum: DatumRef<'a>, from_type: &DataType, into_type: &DataType) -> DatumCow<'a> {
    if from_type == into_type {
        return DatumCow::Borrowed(datum);
    }

    let Some(scalar) = datum else {
        return DatumCow::NULL;
    };

    match (from_type, into_type) {
        (data_types::simple!(), data_types::simple!()) => DatumCow::Borrowed(Some(scalar)),

        (DataType::List(from_inner_type), DataType::List(into_inner_type)) => {
            let list = scalar.into_list();

            let mut builder = into_inner_type.create_array_builder(list.len());
            for datum in list.iter() {
                let datum = do_map(datum, from_inner_type, into_inner_type);
                builder.append(datum);
            }
            let list = ListValue::new(builder.finish());

            DatumCow::Owned(Some(ScalarImpl::List(list)))
        }

        (DataType::Map(from_map_type), DataType::Map(into_map_type)) => {
            let map = scalar.into_map();
            let (keys, values) = map.into_kv();

            let mut value_builder = into_map_type.value().create_array_builder(map.len());
            for value in values.iter() {
                let value = do_map(value, from_map_type.value(), into_map_type.value());
                value_builder.append(value);
            }
            let values = ListValue::new(value_builder.finish());

            let map = MapValue::try_from_kv(keys.to_owned(), values).unwrap();

            DatumCow::Owned(Some(ScalarImpl::Map(map)))
        }

        (DataType::Struct(from_struct_type), DataType::Struct(into_struct_type)) => {
            let struct_value = scalar.into_struct();
            let mut fields = Vec::with_capacity(into_struct_type.len());

            for (id, into_field_type) in into_struct_type
                .ids()
                .unwrap()
                .zip_eq_fast(into_struct_type.types())
            {
                let index = from_struct_type.ids().unwrap().position(|x| x == id);

                let field = if let Some(index) = index {
                    let from_field_type = from_struct_type.type_at(index);
                    let field = struct_value.field_at(index);
                    do_map(field, from_field_type, into_field_type).to_owned_datum()
                } else {
                    // NULL
                    None
                };

                fields.push(field);
            }

            let struct_value = StructValue::new(fields);

            DatumCow::Owned(Some(ScalarImpl::Struct(struct_value)))
        }

        _ => unreachable!(),
    }
}

// #[function("rw_schema_map(any) -> any", type_infer = "unreachable")]
// fn schema_map(source: Datum, ctx: &Context) -> Option<Datum> {
//     let from_type = &ctx.arg_types[0];
//     let to_type = &ctx.return_type;
// }
