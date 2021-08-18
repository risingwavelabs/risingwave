use crate::array::ArrayRef;
use crate::error::Result;
use crate::expr::Datum;
use crate::types::{DataType, DataTypeKind, DataTypeRef, DateType};
use chrono::{Datelike, NaiveDate};
use std::sync::Arc;

pub(crate) fn vec_cast(
    un_casted_array: ArrayRef,
    src_type: DataTypeRef,
    dst_type: DataTypeRef,
) -> Result<ArrayRef> {
    match (src_type.data_type_kind(), dst_type.data_type_kind()) {
        (DataTypeKind::Char, DataTypeKind::Date) => cast_string_to_date(un_casted_array),

        _ => unimplemented!(),
    }
}

fn cast_string_to_date(input: ArrayRef) -> Result<ArrayRef> {
    let mut target_builder = DataType::create_array_builder(
        Arc::new(DateType::new(input.data_type().is_nullable())),
        input.len(),
    )?;
    for _i in 0..input.len() {
        // TODO: Iterate element from input Array (String Array)
        let elem = "1970-01-01";
        let dates_internal =
            NaiveDate::parse_from_str(elem, "%Y-%m-%d").map(|ret| ret.num_days_from_ce())?;
        target_builder.append(&Datum::Int32(dates_internal))?;
    }
    target_builder.finish()
}
