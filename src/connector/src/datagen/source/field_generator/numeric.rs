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
use rand::{thread_rng, Rng};
use serde_json::json;

use super::{DEFAULT_END, DEFAULT_MAX, DEFAULT_MIN, DEFAULT_START};
use crate::datagen::source::field_generator::{FieldKind, NumericFieldGenerator};

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
    ($({ $variant_name:ident, $field_type:ty }),*) => {
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

#[derive(Default)]
pub struct NumericFieldConcrete<T> {
    kind: FieldKind,
    min: T,
    max: T,
    start: T,
    end: T,
    cur: T,
    split_index: u64,
    split_num: u64,
}

impl<T> NumericFieldGenerator for NumericFieldConcrete<T>
where
    T: NumericType,
    <T as FromStr>::Err: std::error::Error + Send + Sync + 'static,
{
    fn with_random(min_option: Option<String>, max_option: Option<String>) -> Result<Self> {
        let mut min = T::DEFAULT_MIN;
        let mut max = T::DEFAULT_MAX;

        if let Some(min_option) = min_option {
            min = min_option.parse::<T>()?;
        }
        if let Some(max_option) = max_option {
            max = max_option.parse::<T>()?;
        }

        assert!(min < max);

        Ok(Self {
            kind: FieldKind::Random,
            min,
            max,
            ..Default::default()
        })
    }

    fn with_sequence(
        star_optiont: Option<String>,
        end_option: Option<String>,
        split_index: u64,
        split_num: u64,
    ) -> Result<Self> {
        let mut start = T::DEFAULT_START;
        let mut end = T::DEFAULT_END;

        if let Some(star_optiont) = star_optiont {
            start = star_optiont.parse::<T>()?;
        }
        if let Some(end_option) = end_option {
            end = end_option.parse::<T>()?;
        }

        assert!(start < end);

        Ok(Self {
            kind: FieldKind::Sequence,
            start,
            end,
            split_index,
            split_num,
            ..Default::default()
        })
    }

    fn generate(&mut self) -> serde_json::Value {
        match self.kind {
            FieldKind::Random => {
                let mut rng = thread_rng();
                let result = rng.gen_range(self.min..=self.max);
                json!(result)
            }
            FieldKind::Sequence => {
                let partition_result = self.start
                    + T::from(self.split_index).unwrap()
                    + T::from(self.split_num).unwrap() * self.cur;
                let partition_result = if partition_result > self.end {
                    None
                } else {
                    Some(partition_result)
                };
                self.cur += T::one();
                json!(partition_result)
            }
        }
    }
}

#[macro_export]
macro_rules! for_all_fields_variants {
    ($macro:ident) => {
        $macro! {
            { I16Field,i16 },
            { I32Field,i32 },
            { I64Field,i64 },
            { F32Field,f32 },
            { F64Field,f64 }
        }
    };
}

macro_rules! gen_field_alias {
    ($({ $variant_name:ident, $field_type:ty }),*) => {
        $(
            pub type $variant_name = NumericFieldConcrete<$field_type>;
        )*
    };
}

for_all_fields_variants! { impl_numeric_type }
for_all_fields_variants! { gen_field_alias }

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_field_generator_with_sequence() {
        let mut i16_field =
            I16Field::with_sequence(Some("5".to_string()), Some("10".to_string()), 0, 1).unwrap();
        for i in 5..=10 {
            assert_eq!(i16_field.generate(), json!(i));
        }
    }
    #[test]
    fn test_field_generator_with_random() {
        let mut i64_field =
            I64Field::with_random(Some("5".to_string()), Some("10".to_string())).unwrap();
        for _ in 0..100 {
            let res = i64_field.generate();
            assert!(res.is_number());
            let res = res.as_i64().unwrap();
            assert!((5..=10).contains(&res));
        }
    }
}
