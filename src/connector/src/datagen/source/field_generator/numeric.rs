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

use std::fmt::Debug;
use std::str::FromStr;

use anyhow::Result;
use rand::distributions::uniform::SampleUniform;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use serde_json::json;

use super::{DEFAULT_END, DEFAULT_MAX, DEFAULT_MIN, DEFAULT_START};
use crate::datagen::source::field_generator::{
    NumericFieldRandomGenerator, NumericFieldSequenceGenerator,
};

trait NumericType
where
    Self: FromStr
        + Copy
        + Debug
        + Default
        + PartialOrd
        + num_traits::Num
        + num_traits::NumAssignOps
        + num_traits::NumCast
        + serde::Serialize
        + SampleUniform,
{
    const DEFAULT_MIN: Self;
    const DEFAULT_MAX: Self;
    const DEFAULT_START: Self;
    const DEFAULT_END: Self;
}

macro_rules! impl_numeric_type {
    ($({ $random_variant_name:ident, $sequence_variant_name:ident,$field_type:ty }),*) => {
        $(
            impl NumericType for $field_type {
                const DEFAULT_MIN: $field_type = DEFAULT_MIN as $field_type;
                const DEFAULT_MAX: $field_type = DEFAULT_MAX as $field_type;
                const DEFAULT_START: $field_type = DEFAULT_START as $field_type;
                const DEFAULT_END: $field_type = DEFAULT_END as $field_type;
            }
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
    T: NumericType,
    <T as FromStr>::Err: std::error::Error + Send + Sync + 'static,
{
    fn new(min_option: Option<String>, max_option: Option<String>, seed: u64) -> Result<Self>
    where
        Self: Sized,
    {
        let mut min = T::DEFAULT_MIN;
        let mut max = T::DEFAULT_MAX;

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
}
impl<T> NumericFieldSequenceGenerator for NumericFieldSequenceConcrete<T>
where
    T: NumericType,
    <T as FromStr>::Err: std::error::Error + Send + Sync + 'static,
{
    fn new(
        star_option: Option<String>,
        end_option: Option<String>,
        offset: u64,
        step: u64,
    ) -> Result<Self>
    where
        Self: Sized,
    {
        let mut start = T::DEFAULT_START;
        let mut end = T::DEFAULT_END;

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
            ..Default::default()
        })
    }

    fn generate(&mut self) -> serde_json::Value {
        let partition_result =
            self.start + T::from(self.offset).unwrap() + T::from(self.step).unwrap() * self.cur;
        let partition_result = if partition_result > self.end {
            None
        } else {
            Some(partition_result)
        };
        self.cur += T::one();
        json!(partition_result)
    }
}

#[macro_export]
macro_rules! for_all_fields_variants {
    ($macro:ident) => {
        $macro! {
            { I16RandomField,I16SequenceField,i16 },
            { I32RandomField,I32SequenceField,i32 },
            { I64RandomField,I64SequenceField,i64 },
            { F32RandomField,F32SequenceField,f32 },
            { F64RandomField,F64SequenceField,f64 }
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
    #[test]
    fn test_sequence_field_generator() {
        let mut i16_field =
            I16SequenceField::new(Some("5".to_string()), Some("10".to_string()), 0, 1).unwrap();
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
    }
}
