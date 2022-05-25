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

use anyhow::Result;
use rand::{thread_rng, Rng};
use serde_json::json;

use super::{DEFAULT_END, DEFAULT_MAX, DEFAULT_MIN, DEFAULT_START};
use crate::datagen::source::field_generator::{FieldKind, NumericFieldGenerator};

#[macro_export]
macro_rules! impl_field_generator {
    ($({ $variant_name:ident, $field_type:ty }),*) => {
        $(
            #[derive(Default)]
            pub struct $variant_name {
                kind: FieldKind,
                min: $field_type,
                max: $field_type,
                start: $field_type,
                end: $field_type,
                events_so_far: u64,
                split_index: $field_type,
                split_num: $field_type,
            }

            impl NumericFieldGenerator for $variant_name {
                fn with_random(min_option: Option<String>, max_option: Option<String>) -> Result<Self> {

                    let mut min = DEFAULT_MIN as $field_type;
                    let mut max = DEFAULT_MAX as $field_type;

                    if let Some(min_option) = min_option {
                        min = min_option.parse::<$field_type>()?;
                    }
                    if let Some(max_option) = max_option {
                        max = max_option.parse::<$field_type>()?;
                    }

                    assert!(min < max);

                    Ok(Self {
                        kind: FieldKind::Random,
                        min,
                        max,
                        ..Default::default()
                    })
                }

                fn with_sequence(star_optiont: Option<String>, end_option: Option<String>,split_index:i32,split_num:i32) -> Result<Self> {

                    let mut start = DEFAULT_START as $field_type;
                    let mut end = DEFAULT_END as $field_type;

                    if let Some(star_optiont) = star_optiont {
                        start = star_optiont.parse::<$field_type>()?;
                    }
                    if let Some(end_option) = end_option {
                        end = end_option.parse::<$field_type>()?;
                    }

                    assert!(start < end);

                    Ok(Self {
                        kind: FieldKind::Sequence,
                        start,
                        end,
                        split_index: split_index as $field_type,
                        split_num: split_num as $field_type,
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
                            let events_so_far = self.events_so_far as $field_type;
                            let partition_result = self.start + self.split_index + self.split_num*events_so_far;
                            let partition_result = self.end.min(partition_result);
                            self.events_so_far += 1;
                            json!(partition_result)
                        }
                    }
                }
            }
        )*
    };
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

for_all_fields_variants! {impl_field_generator}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_field_generator_with_sequence() {
        let mut i16_field =
            I16Field::with_sequence(Some("5".to_string()), Some("10".to_string()),0,1).unwrap();
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
