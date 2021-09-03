use crate::array::{ArrayRef, PrimitiveArrayBuilder, UTF8Array};
use crate::error::Result;
use crate::types::{DataType, DataTypeKind, DataTypeRef, DateType};
use crate::util::{downcast_mut, downcast_ref};
use chrono::{Datelike, NaiveDate};
use std::sync::Arc;

pub fn vec_cast(un_casted_array: ArrayRef, dst_type: DataTypeRef) -> Result<ArrayRef> {
    match (
        un_casted_array.data_type().data_type_kind(),
        dst_type.data_type_kind(),
    ) {
        (DataTypeKind::Char, DataTypeKind::Date) => cast_string_to_date(un_casted_array),

        _ => unimplemented!(),
    }
}

// The same as NaiveDate::from_ymd(1970, 1, 1).num_days_from_ce().
// Minus this magic number to store the number of days since 1970-01-01.
const UNIX_EPOCH_DAYS: i32 = 719_163;

fn cast_string_to_date(input: ArrayRef) -> Result<ArrayRef> {
    let mut boxed_target_builder = DataType::create_array_builder(
        Arc::new(DateType::new(input.data_type().is_nullable())),
        input.len(),
    )?;
    let target_builder: &mut PrimitiveArrayBuilder<DateType> =
        downcast_mut(boxed_target_builder.as_mut())?;
    let utf8_arr: &UTF8Array = downcast_ref(&*input)?;
    let utf8_iter = utf8_arr.as_iter()?;
    for ret in utf8_iter {
        match ret {
            Some(elem) => {
                let dates_internal = NaiveDate::parse_from_str(elem, "%Y-%m-%d")
                    .map(|ret| ret.num_days_from_ce() - UNIX_EPOCH_DAYS)?;
                target_builder.append_value(Some(dates_internal))?;
            }
            _ => {
                target_builder.append_value(None)?;
            }
        }
    }
    boxed_target_builder.finish()
}
