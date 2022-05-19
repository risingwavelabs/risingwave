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

use super::{FieldGenerator, FieldKind};
#[derive(Default)]
pub struct IntField {
    kind: FieldKind,
    min: i32,
    max: i32,
    start: i32,
    end: i32,
    last: Option<i32>,
}

impl FieldGenerator for IntField {
    fn with_random(min_option: Option<String>, max_option: Option<String>) -> Result<Self> {
        let mut min = 0;
        let mut max = 1000;
        if let Some(min_option) = min_option {
            min = min_option.parse::<i32>()?;
        }
        if let Some(max_option) = max_option {
            max = max_option.parse::<i32>()?;
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
        let mut start = 0;
        let mut end = 1000;
        if let Some(star_optiont) = star_optiont {
            start = star_optiont.parse::<i32>()?;
        }
        if let Some(end_option) = end_option {
            end = end_option.parse::<i32>()?;
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
                    let res = self.end.min(last + 1);
                    self.last = Some(last + 1);
                    json!(res)
                } else {
                    self.last = Some(self.start);
                    json!(self.start)
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_field_generator_with_sequence() {
        let mut i_seq =
            IntField::with_sequence(Some("5".to_string()), Some("10".to_string())).unwrap();
        for i in 5..=10 {
            assert_eq!(i_seq.generate(), json!(i));
        }
    }
    #[test]
    fn test_field_generator_with_random() {
        let mut i_seq =
            IntField::with_random(Some("5".to_string()), Some("10".to_string())).unwrap();
        for _ in 0..100 {
            let res = i_seq.generate();
            assert!(res.is_number());
            let res = res.as_i64().unwrap();
            assert!((5..=10).contains(&res));
        }
    }
}
