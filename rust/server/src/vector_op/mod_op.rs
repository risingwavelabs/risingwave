use crate::error::Result;

// Rust allows only mod between same type.
macro_rules! mod_primitive_types {
    ($fn_name: ident, $left_type: ty, $right_type: ty, $max_type: ty) => {
        pub(crate) fn $fn_name(
            v1: Option<$left_type>,
            v2: Option<$right_type>,
        ) -> Result<Option<$max_type>> {
            match (v1, v2) {
                (Some(l1), Some(l2)) => Ok(Some((l1 as $max_type) % (l2 as $max_type))),
                _ => Ok(None),
            }
        }
    };
}

mod_primitive_types!(mod_i16_i16, i16, i16, i16);
mod_primitive_types!(mod_i16_i32, i16, i32, i32);
mod_primitive_types!(mod_i16_i64, i16, i64, i64);
mod_primitive_types!(mod_i16_f32, i16, f32, f32);
mod_primitive_types!(mod_i16_f64, i16, f64, f64);

mod_primitive_types!(mod_i32_i16, i32, i16, i16);
mod_primitive_types!(mod_i32_i32, i32, i32, i32);
mod_primitive_types!(mod_i32_i64, i32, i64, i64);
mod_primitive_types!(mod_i32_f32, i32, f32, f32);
mod_primitive_types!(mod_i32_f64, i32, f64, f64);

mod_primitive_types!(mod_i64_i16, i64, i16, i64);
mod_primitive_types!(mod_i64_i32, i64, i32, i64);
mod_primitive_types!(mod_i64_i64, i64, i64, i64);
mod_primitive_types!(mod_i64_f32, i64, f32, f32);
mod_primitive_types!(mod_i64_f64, i64, f64, f64);

mod_primitive_types!(mod_f32_i16, f32, i16, f32);
mod_primitive_types!(mod_f32_i32, f32, i32, f32);
mod_primitive_types!(mod_f32_i64, f32, i64, f32);
mod_primitive_types!(mod_f32_f32, f32, f32, f32);
mod_primitive_types!(mod_f32_f64, f32, f64, f64);

mod_primitive_types!(mod_f64_i16, f64, i16, f64);
mod_primitive_types!(mod_f64_i32, f64, i32, f64);
mod_primitive_types!(mod_f64_i64, f64, i64, f64);
mod_primitive_types!(mod_f64_f32, f64, f32, f64);
mod_primitive_types!(mod_f64_f64, f64, f64, f64);
