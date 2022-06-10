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

use std::collections::HashSet;
use std::fmt::Debug;
use std::sync::Arc;

use risingwave_common::array::{ArrayBuilder, ArrayRef, BoolArrayBuilder, DataChunk, Row};
use risingwave_common::types::{DataType, Datum, Scalar, ToOwnedDatum};

use crate::expr::{BoxedExpression, Expression};
use crate::{ExprError, Result};

#[derive(Debug)]
pub(crate) struct InExpression {
    left: BoxedExpression,
    set: HashSet<Datum>,
    return_type: DataType,
}

impl InExpression {
    pub fn new(
        left: BoxedExpression,
        data: impl Iterator<Item = Datum>,
        return_type: DataType,
    ) -> Self {
        let mut sarg = HashSet::new();
        for datum in data {
            sarg.insert(datum);
        }
        Self {
            left,
            set: sarg,
            return_type,
        }
    }

    fn exists(&self, datum: &Datum) -> bool {
        self.set.contains(datum)
    }
}

impl Expression for InExpression {
    fn return_type(&self) -> DataType {
        self.return_type.clone()
    }

    fn eval(&self, input: &DataChunk) -> Result<ArrayRef> {
        let input_array = self.left.eval(input)?;
        let mut output_array = BoolArrayBuilder::new(input_array.len())?;
        for data in input_array.iter() {
            let ret = self.exists(&data.to_owned_datum());
            output_array.append(Some(ret))?;
        }
        Ok(Arc::new(output_array.finish()?.into()))
    }

    fn eval_row(&self, input: &Row) -> Result<Datum> {
        let data = self.left.eval_row(input)?;
        let ret = self.exists(&data);
        Ok(Some(ret.to_scalar_value()))
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::array::{DataChunk, Row};
    use risingwave_common::test_prelude::DataChunkTestExt;
    use risingwave_common::types::{DataType, Scalar, ScalarImpl};

    use crate::expr::expr_in::InExpression;
    use crate::expr::{Expression, InputRefExpression};

    #[test]
    fn test_eval_search_expr() {
        let input_ref = Box::new(InputRefExpression::new(DataType::Varchar, 0));
        let data = vec![
            Some(ScalarImpl::Utf8("abc".to_string())),
            Some(ScalarImpl::Utf8("def".to_string())),
        ];
        let search_expr = InExpression::new(input_ref, data.into_iter(), DataType::Boolean);
        let data_chunk = DataChunk::from_pretty(
            "T
             abc
             a
             def
             abc",
        )
        .with_invisible_holes();
        let res = search_expr.eval(&data_chunk).unwrap();
        assert_eq!(res.datum_at(0), Some(ScalarImpl::Bool(true)));
        assert_eq!(res.datum_at(1), Some(ScalarImpl::Bool(false)));
        assert_eq!(res.datum_at(2), Some(ScalarImpl::Bool(true)));
        assert_eq!(res.datum_at(3), Some(ScalarImpl::Bool(true)));
    }

    #[test]
    fn test_eval_row_search_expr() {
        let input_ref = Box::new(InputRefExpression::new(DataType::Varchar, 0));
        let data = vec![
            Some(ScalarImpl::Utf8("abc".to_string())),
            Some(ScalarImpl::Utf8("def".to_string())),
        ];
        let search_expr = InExpression::new(input_ref, data.into_iter(), DataType::Boolean);

        let row_inputs = vec!["abc", "a", "def"];
        let expected = vec![true, false, true];

        for (i, row_input) in row_inputs.iter().enumerate() {
            let row = Row::new(vec![Some(row_input.to_string().to_scalar_value())]);
            let result = search_expr.eval_row(&row).unwrap().unwrap().into_bool();
            assert_eq!(result, expected[i]);
        }
    }
}
