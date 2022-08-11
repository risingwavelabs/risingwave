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

use std::borrow::Cow;
use std::cell::Ref;
use std::fmt::{Debug, Display, Formatter};
use std::ops::{Add, Div, Mul, Neg, Rem, Sub};
use std::rc::Rc;

use bigdecimal::BigDecimal as RustDecimal;
use num_traits::{CheckedAdd, CheckedDiv, CheckedMul, CheckedNeg, CheckedRem, CheckedSub, Zero};

#[derive(Debug, Clone, PartialEq, Eq, Ord, PartialOrd)]
pub enum Decimal {
    Normalized(RustDecimal),
    NaN,
    PositiveINF,
    NegativeINF,
}

macro_rules! impl_from_float {
    ([$(($T:ty, $from_float:ident)), *]) => {
        $(fn $from_float(num: $T) -> Option<Decimal> {
            match num {
                num if num.is_nan() => Some(Decimal::NaN),
                num if num.is_infinite() && num.is_sign_positive() => Some(Decimal::PositiveINF),
                num if num.is_infinite() && num.is_sign_negative() => Some(Decimal::NegativeINF),
                num => {
                    Some(Decimal::Normalized(RustDecimal::new(0.into(), 0)))
                    // Some(Decimal::Normalized(&RustDecimal::try_from(num).unwrap()))
                },
            }
        })*
    }
}

impl Decimal {
    impl_from_float!([(f32, from_f32), (f64, from_f64)]);
}

// #[cfg(test)]
// mod tests {
//     use itertools::Itertools;

//     use super::*;

//     fn check(lhs: f32, rhs: f32) -> bool {
//         if lhs.is_nan() && rhs.is_nan() {
//             true
//         } else if lhs.is_infinite() && rhs.is_infinite() {
//             if lhs.is_sign_positive() && rhs.is_sign_positive() {
//                 true
//             } else {
//                 lhs.is_sign_negative() && rhs.is_sign_negative()
//             }
//         } else if lhs.is_finite() && rhs.is_finite() {
//             lhs == rhs
//         } else {
//             false
//         }
//     }

//     #[test]
//     fn check_op_with_float() {
//         let decimals = [
//             Decimal::NaN,
//             Decimal::PositiveINF,
//             Decimal::NegativeINF,
//             Decimal::from_f32(1.0).unwrap(),
//             Decimal::from_f32(-1.0).unwrap(),
//             Decimal::from_f32(0.0).unwrap(),
//         ];
//         let floats = [
//             f32::NAN,
//             f32::INFINITY,
//             f32::NEG_INFINITY,
//             1.0f32,
//             -1.0f32,
//             0.0f32,
//         ];
//         for (d_lhs, f_lhs) in decimals.iter().zip_eq(floats.iter()) {
//             for (d_rhs, f_rhs) in decimals.iter().zip_eq(floats.iter()) {
//                 assert!(check((*d_lhs + *d_rhs).to_f32().unwrap(), f_lhs + f_rhs));
//                 assert!(check((*d_lhs - *d_rhs).to_f32().unwrap(), f_lhs - f_rhs));
//                 assert!(check((*d_lhs * *d_rhs).to_f32().unwrap(), f_lhs * f_rhs));
//                 assert!(check((*d_lhs / *d_rhs).to_f32().unwrap(), f_lhs / f_rhs));
//                 assert!(check((*d_lhs % *d_rhs).to_f32().unwrap(), f_lhs % f_rhs));
//             }
//         }
//     }

//     #[test]
//     fn basic_test() {
//         assert_eq!(Decimal::from_str("nan").unwrap(), Decimal::NaN,);
//         assert_eq!(Decimal::from_str("NaN").unwrap(), Decimal::NaN,);
//         assert_eq!(Decimal::from_str("NAN").unwrap(), Decimal::NaN,);

//         assert_eq!(Decimal::from_str("inf").unwrap(), Decimal::PositiveINF,);
//         assert_eq!(Decimal::from_str("INF").unwrap(), Decimal::PositiveINF,);
//         assert_eq!(Decimal::from_str("+inf").unwrap(), Decimal::PositiveINF,);
//         assert_eq!(Decimal::from_str("+INF").unwrap(), Decimal::PositiveINF,);
//         assert_eq!(Decimal::from_str("+Inf").unwrap(), Decimal::PositiveINF,);

//         assert_eq!(Decimal::from_str("-inf").unwrap(), Decimal::NegativeINF,);
//         assert_eq!(Decimal::from_str("-INF").unwrap(), Decimal::NegativeINF,);
//         assert_eq!(Decimal::from_str("-Inf").unwrap(), Decimal::NegativeINF,);

//         assert!(Decimal::from_str("nAn").is_err());
//         assert!(Decimal::from_str("nAN").is_err());
//         assert!(Decimal::from_str("Nan").is_err());
//         assert!(Decimal::from_str("NAn").is_err());

//         assert!(Decimal::from_str("iNF").is_err());
//         assert!(Decimal::from_str("inF").is_err());
//         assert!(Decimal::from_str("InF").is_err());
//         assert!(Decimal::from_str("INf").is_err());

//         assert!(Decimal::from_str("+iNF").is_err());
//         assert!(Decimal::from_str("+inF").is_err());
//         assert!(Decimal::from_str("+InF").is_err());
//         assert!(Decimal::from_str("+INf").is_err());

//         assert!(Decimal::from_str("-iNF").is_err());
//         assert!(Decimal::from_str("-inF").is_err());
//         assert!(Decimal::from_str("-InF").is_err());
//         assert!(Decimal::from_str("-INf").is_err());

//         assert_eq!(
//             Decimal::from_f32(10.0).unwrap() / Decimal::PositiveINF,
//             Decimal::from_f32(0.0).unwrap(),
//         );
//         assert_eq!(
//             Decimal::from_f32(f32::INFINITY).unwrap(),
//             Decimal::PositiveINF
//         );
//         assert_eq!(Decimal::from_f64(f64::NAN).unwrap(), Decimal::NaN);
//         assert_eq!(
//             Decimal::from_f64(f64::INFINITY).unwrap(),
//             Decimal::PositiveINF
//         );
//         assert_eq!(
//
// Decimal::unordered_deserialize(Decimal::from_f64(1.234).unwrap().unordered_serialize()),
//             Decimal::from_f64(1.234).unwrap(),
//         );
//         assert_eq!(
//             Decimal::unordered_deserialize(Decimal::from_u8(1).unwrap().unordered_serialize()),
//             Decimal::from_u8(1).unwrap(),
//         );
//         assert_eq!(
//             Decimal::unordered_deserialize(Decimal::from_i8(1).unwrap().unordered_serialize()),
//             Decimal::from_i8(1).unwrap(),
//         );
//         assert_eq!(
//             Decimal::unordered_deserialize(Decimal::from_u16(1).unwrap().unordered_serialize()),
//             Decimal::from_u16(1).unwrap(),
//         );
//         assert_eq!(
//             Decimal::unordered_deserialize(Decimal::from_i16(1).unwrap().unordered_serialize()),
//             Decimal::from_i16(1).unwrap(),
//         );
//         assert_eq!(
//             Decimal::unordered_deserialize(Decimal::from_u32(1).unwrap().unordered_serialize()),
//             Decimal::from_u32(1).unwrap(),
//         );
//         assert_eq!(
//             Decimal::unordered_deserialize(Decimal::from_i32(1).unwrap().unordered_serialize()),
//             Decimal::from_i32(1).unwrap(),
//         );
//         assert_eq!(
//             Decimal::unordered_deserialize(
//                 Decimal::from_f64(f64::NAN).unwrap().unordered_serialize()
//             ),
//             Decimal::from_f64(f64::NAN).unwrap(),
//         );
//         assert_eq!(
//             Decimal::unordered_deserialize(
//                 Decimal::from_f64(f64::INFINITY)
//                     .unwrap()
//                     .unordered_serialize()
//             ),
//             Decimal::from_f64(f64::INFINITY).unwrap(),
//         );
//         assert_eq!(Decimal::to_u8(&Decimal::from_u8(1).unwrap()).unwrap(), 1,);
//         assert_eq!(Decimal::to_i8(&Decimal::from_i8(1).unwrap()).unwrap(), 1,);
//         assert_eq!(Decimal::to_u16(&Decimal::from_u16(1).unwrap()).unwrap(), 1,);
//         assert_eq!(Decimal::to_i16(&Decimal::from_i16(1).unwrap()).unwrap(), 1,);
//         assert_eq!(Decimal::to_u32(&Decimal::from_u32(1).unwrap()).unwrap(), 1,);
//         assert_eq!(Decimal::to_i32(&Decimal::from_i32(1).unwrap()).unwrap(), 1,);
//         assert_eq!(Decimal::to_u64(&Decimal::from_u64(1).unwrap()).unwrap(), 1,);
//         assert_eq!(Decimal::to_i64(&Decimal::from_i64(1).unwrap()).unwrap(), 1,);
//     }
// }
