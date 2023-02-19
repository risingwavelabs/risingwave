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

use std::fmt::Write;
use std::sync::Arc;

use risingwave_common::array::{Array, ArrayBuilder, NaiveDateTimeArray, Utf8ArrayBuilder};
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{DataType, Datum, ScalarImpl};
use risingwave_common::util::iter_util::ZipEqFast;

use super::Expression;
use crate::vector_op::to_char::ChronoPattern;

#[derive(Debug)]
pub(crate) struct ExprToCharConstTmplContext {
    pub(crate) chrono_pattern: ChronoPattern,
}

#[derive(Debug)]
pub(crate) struct ExprToCharConstTmpl {
    pub(crate) child: Box<dyn Expression>,
    pub(crate) ctx: ExprToCharConstTmplContext,
}

impl Expression for ExprToCharConstTmpl {
    fn return_type(&self) -> DataType {
        DataType::Varchar
    }

    fn eval(
        &self,
        input: &risingwave_common::array::DataChunk,
    ) -> crate::Result<risingwave_common::array::ArrayRef> {
        let data_arr = self.child.eval_checked(input)?;
        let data_arr: &NaiveDateTimeArray = data_arr.as_ref().into();
        let mut output = Utf8ArrayBuilder::new(input.capacity());
        for (data, vis) in data_arr.iter().zip_eq_fast(input.vis().iter()) {
            if !vis {
                output.append_null();
            } else if let Some(data) = data {
                let mut writer = output.writer().begin();
                let fmt = data
                    .0
                    .format_with_items(self.ctx.chrono_pattern.borrow_items().iter());
                write!(writer, "{fmt}").unwrap();
                writer.finish();
            } else {
                output.append_null();
            }
        }

        Ok(Arc::new(output.finish().into()))
    }

    fn eval_row(&self, input: &OwnedRow) -> crate::Result<Datum> {
        let data = self.child.eval_row(input)?;
        Ok(if let Some(ScalarImpl::NaiveDateTime(data)) = data {
            Some(
                data.0
                    .format_with_items(self.ctx.chrono_pattern.borrow_items().iter())
                    .to_string()
                    .into(),
            )
        } else {
            None
        })
    }
}
