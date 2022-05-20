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
use risingwave_common::types::DataType;
use serde_json::{json, Value};

use super::{FieldGenerator, FieldKind};

pub enum FieldGeneratorImpl {
    I32(I32Field),
    // I32(Box::new(I32Field)),
    // I64(Box::new(I64Field)),
    F32(F32Field),
    // F64(Box::new(F64Field)),
}
impl FieldGeneratorImpl {
    pub fn new(
        data_type: DataType,
        kind: FieldKind,
        min_or_start: Option<String>,
        max_or_end: Option<String>,
    ) -> Result<Self> {
        match kind {
            FieldKind::Random => match data_type {
                DataType::Int32 => Ok(FieldGeneratorImpl::I32(I32Field::with_random(
                    min_or_start,
                    max_or_end,
                )?)),
                DataType::Float32 => Ok(FieldGeneratorImpl::F32(F32Field::with_random(
                    min_or_start,
                    max_or_end,
                )?)),
                _ => todo!(),
            },
            FieldKind::Sequence => match data_type {
                DataType::Int32 => Ok(FieldGeneratorImpl::I32(I32Field::with_sequence(
                    min_or_start,
                    max_or_end,
                )?)),
                DataType::Float32 => Ok(FieldGeneratorImpl::F32(F32Field::with_random(
                    min_or_start,
                    max_or_end,
                )?)),
                _ => todo!(),
            },
        }
    }

    pub fn generate(&mut self) -> Value {
        match self {
            FieldGeneratorImpl::I32(f) => f.generate(),
            FieldGeneratorImpl::F32(f) => f.generate(),
            _ => todo!(),
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
                last: Option<$field_type>,
            }

            impl FieldGenerator for $variant_name {
                fn with_random(min_option: Option<String>, max_option: Option<String>) -> Result<Self> {

                    //FIXME should reconsider default value
                    let mut min = i16::MIN as $field_type;
                    let mut max = i16::MAX as $field_type;

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

                fn with_sequence(star_optiont: Option<String>, end_option: Option<String>) -> Result<Self> {

                    //FIXME should reconsider default value
                    let mut start = i16::MIN as $field_type;
                    let mut end = i16::MAX as $field_type;

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
                        ..Default::default()
                    })
                }

                fn generate(&mut self) -> serde_json::Value {
                    match self.kind {
                        FieldKind::Random => {
                            let mut rng = thread_rng();
                            let res = rng.gen_range(self.min..=self.max);
                            json!(res)
                        }
                        FieldKind::Sequence => {
                            if let Some(last) = self.last {
                                let res = self.end.min(last + (1 as $field_type));
                                self.last = Some(last + (1 as $field_type));
                                json!(res)
                            } else {
                                self.last = Some(self.start);
                                json!(self.start)
                            }
                        }
                    }
                }
            }
        )*
    };
}

for_all_fields_variants! {impl_field_generator}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_field_generator_with_sequence() {
        let mut i_seq =
            I16Field::with_sequence(Some("5".to_string()), Some("10".to_string())).unwrap();
        for i in 5..=10 {
            assert_eq!(i_seq.generate(), json!(i));
        }
    }
    #[test]
    fn test_field_generator_with_random() {
        let mut i_seq =
            I64Field::with_random(Some("5".to_string()), Some("10".to_string())).unwrap();
        for _ in 0..100 {
            let res = i_seq.generate();
            assert!(res.is_number());
            let res = res.as_i64().unwrap();
            assert!((5..=10).contains(&res));
        }
    }
    #[test]
    fn test_macro() {
        let i32_field = FieldGeneratorImpl::new(
            DataType::Int32,
            FieldKind::Sequence,
            Some("5".to_string()),
            Some("10".to_string()),
        )
        .unwrap();
        let f32_field = FieldGeneratorImpl::new(
            DataType::Float32,
            FieldKind::Random,
            Some("5".to_string()),
            Some("10".to_string()),
        )
        .unwrap();
        let mut fields = vec![i32_field, f32_field];
        for _ in 0..10 {
            for field in &mut fields{
                dbg!(field.generate());
            }
        }
    }
}
