// Copyright 2023 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fmt::Debug;
use std::str::FromStr;

use anyhow::Result;
use rand::distributions::uniform::SampleUniform;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use serde_json::json;

use crate::field_generator::{NumericFieldRandomGenerator, NumericFieldSequenceGenerator};
use crate::types::{Datum, Scalar, F32, F64};

trait NumericType
where
    Self: FromStr
        + Copy
        + Debug
        + Default
        + PartialOrd
        + num_traits::Num
        + num_traits::NumAssignOps
        + From<i16>
        + TryFrom<u64>
        + serde::Serialize
        + SampleUniform,
{
}

macro_rules! impl_numeric_type {
    ($({ $random_variant_name:ident, $sequence_variant_name:ident,$field_type:ty }),*) => {
        $(
            impl NumericType for $field_type {}
        )*
    };
}

pub struct NumericFieldRandomConcrete<T> {
    min: T,
    max: T,
    seed: u64,
}

#[derive(Default)]
pub struct NumericFieldSequenceConcrete<T> {
    start: T,
    end: T,
    cur: T,
    offset: u64,
    step: u64,
}

impl<T> NumericFieldRandomGenerator for NumericFieldRandomConcrete<T>
where
    T: NumericType + Scalar,
    <T as FromStr>::Err: std::error::Error + Send + Sync + 'static,
{
    fn new(min_option: Option<String>, max_option: Option<String>, seed: u64) -> Result<Self>
    where
        Self: Sized,
    {
        let mut min = T::zero();
        let mut max = T::from(i16::MAX);

        if let Some(min_option) = min_option {
            min = min_option.parse::<T>()?;
        }
        if let Some(max_option) = max_option {
            max = max_option.parse::<T>()?;
        }
        assert!(min < max);

        Ok(Self { min, max, seed })
    }

    fn generate(&mut self, offset: u64) -> serde_json::Value {
        let mut rng = StdRng::seed_from_u64(offset ^ self.seed);
        let result = rng.gen_range(self.min..=self.max);
        json!(result)
    }

    fn generate_datum(&mut self, offset: u64) -> Datum {
        let mut rng = StdRng::seed_from_u64(offset ^ self.seed);
        let result = rng.gen_range(self.min..=self.max);
        Some(result.to_scalar_value())
    }
}
impl<T> NumericFieldSequenceGenerator for NumericFieldSequenceConcrete<T>
where
    T: NumericType + Scalar,
    <T as FromStr>::Err: std::error::Error + Send + Sync + 'static,
    <T as TryFrom<u64>>::Error: Debug,
{
    fn new(
        star_option: Option<String>,
        end_option: Option<String>,
        offset: u64,
        step: u64,
        event_offset: u64,
    ) -> Result<Self>
    where
        Self: Sized,
    {
        let mut start = T::zero();
        let mut end = T::from(i16::MAX);

        if let Some(star_optiont) = star_option {
            start = star_optiont.parse::<T>()?;
        }
        if let Some(end_option) = end_option {
            end = end_option.parse::<T>()?;
        }

        assert!(start < end);
        Ok(Self {
            start,
            end,
            offset,
            step,
            cur: T::try_from(event_offset).map_err(|_| {
                anyhow::anyhow!("event offset is too big, offset: {}", event_offset,)
            })?,
        })
    }

    fn generate(&mut self) -> serde_json::Value {
        let partition_result = self.start
            + T::try_from(self.offset).unwrap()
            + T::try_from(self.step).unwrap() * self.cur;
        let partition_result = if partition_result > self.end {
            None
        } else {
            Some(partition_result)
        };
        self.cur += T::one();
        json!(partition_result)
    }

    fn generate_datum(&mut self) -> Datum {
        let partition_result = self.start
            + T::try_from(self.offset).unwrap()
            + T::try_from(self.step).unwrap() * self.cur;
        self.cur += T::one();
        if partition_result > self.end {
            None
        } else {
            Some(partition_result.to_scalar_value())
        }
    }
}

#[macro_export]
macro_rules! for_all_fields_variants {
    ($macro:ident) => {
        $macro! {
            { I16RandomField,I16SequenceField,i16 },
            { I32RandomField,I32SequenceField,i32 },
            { I64RandomField,I64SequenceField,i64 },
            { F32RandomField,F32SequenceField,F32 },
            { F64RandomField,F64SequenceField,F64 }
        }
    };
}

macro_rules! gen_random_field_alias {
    ($({ $random_variant_name:ident, $sequence_variant_name:ident,$field_type:ty }),*) => {
        $(
            pub type $random_variant_name = NumericFieldRandomConcrete<$field_type>;
        )*
    };
}

macro_rules! gen_sequence_field_alias {
    ($({ $random_variant_name:ident, $sequence_variant_name:ident,$field_type:ty }),*) => {
        $(
            pub type $sequence_variant_name = NumericFieldSequenceConcrete<$field_type>;
        )*
    };
}

for_all_fields_variants! { impl_numeric_type }
for_all_fields_variants! { gen_random_field_alias }
for_all_fields_variants! { gen_sequence_field_alias }

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::DefaultOrd;

    #[test]
    fn test_sequence_field_generator() {
        let mut i16_field =
            I16SequenceField::new(Some("5".to_string()), Some("10".to_string()), 0, 1, 0).unwrap();
        for i in 5..=10 {
            assert_eq!(i16_field.generate(), json!(i));
        }
    }
    #[test]
    fn test_random_field_generator() {
        let mut i64_field =
            I64RandomField::new(Some("5".to_string()), Some("10".to_string()), 114).unwrap();
        for i in 0..100 {
            let res = i64_field.generate(i as u64);
            assert!(res.is_number());
            let res = res.as_i64().unwrap();
            assert!((5..=10).contains(&res));
        }

        // test overflow
        let mut i64_field = I64RandomField::new(None, None, 114).unwrap();
        for i in 0..100 {
            let res = i64_field.generate(i as u64);
            assert!(res.is_number());
            let res = res.as_i64().unwrap();
            assert!(res >= 0);
        }
    }
    #[test]
    fn test_sequence_datum_generator() {
        let mut f32_field =
            F32SequenceField::new(Some("5.0".to_string()), Some("10.0".to_string()), 0, 1, 0)
                .unwrap();

        for i in 5..=10 {
            assert_eq!(
                f32_field.generate_datum(),
                Some(F32::from(i as f32).to_scalar_value())
            );
        }
    }
    #[test]
    fn test_random_datum_generator() {
        let mut i32_field =
            I32RandomField::new(Some("-5".to_string()), Some("5".to_string()), 123).unwrap();
        let (lower, upper) = ((-5).to_scalar_value(), 5.to_scalar_value());
        for i in 0..100 {
            let res = i32_field.generate_datum(i as u64);
            assert!(res.is_some());
            let res = res.unwrap();
            assert!(lower.default_cmp(&res).is_le() && res.default_cmp(&upper).is_le());
        }
    }

    #[test]
    fn test_sequence_field_generator_float() {
        let mut f64_field =
            F64SequenceField::new(Some("0".to_string()), Some("10".to_string()), 0, 1, 0).unwrap();
        for i in 0..=10 {
            assert_eq!(f64_field.generate(), json!(i as f64));
        }

        let mut f32_field =
            F32SequenceField::new(Some("-5".to_string()), Some("5".to_string()), 0, 1, 0).unwrap();
        for i in -5..=5 {
            assert_eq!(f32_field.generate(), json!(i as f32));
        }
    }

    #[test]
    fn test_random_field_generator_float() {
        let mut f64_field =
            F64RandomField::new(Some("5".to_string()), Some("10".to_string()), 114).unwrap();
        for i in 0..100 {
            let res = f64_field.generate(i as u64);
            assert!(res.is_number());
            let res = res.as_f64().unwrap();
            assert!((5. ..10.).contains(&res));
        }

        // test overflow
        let mut f64_field = F64RandomField::new(None, None, 114).unwrap();
        for i in 0..100 {
            let res = f64_field.generate(i as u64);
            assert!(res.is_number());
            let res = res.as_f64().unwrap();
            assert!(res >= 0.);
        }

        let mut f32_field =
            F32RandomField::new(Some("5".to_string()), Some("10".to_string()), 114).unwrap();
        for i in 0..100 {
            let res = f32_field.generate(i as u64);
            assert!(res.is_number());
            // it seems there is no `as_f32`...
            let res = res.as_f64().unwrap();
            assert!((5. ..10.).contains(&res));
        }

        // test overflow
        let mut f32_field = F32RandomField::new(None, None, 114).unwrap();
        for i in 0..100 {
            let res = f32_field.generate(i as u64);
            assert!(res.is_number());
            let res = res.as_f64().unwrap();
            assert!(res >= 0.);
        }
    }
}
