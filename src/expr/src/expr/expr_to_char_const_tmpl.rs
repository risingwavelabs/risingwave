use std::sync::Arc;

use itertools::Itertools;
use risingwave_common::array::{Array, ArrayBuilder, NaiveDateTimeArray, Utf8ArrayBuilder};
use risingwave_common::types::{DataType, Datum, ScalarImpl};

use super::Expression;

#[derive(Debug)]
pub(crate) struct ExprToCharConstTmplContext {
    pub(crate) chrono_tmpl: String,
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
        for (data, vis) in data_arr.iter().zip_eq(input.vis().iter()) {
            if !vis {
                output.append_null()?;
            } else {
                if let Some(data) = data {
                    let res = data.0.format(&self.ctx.chrono_tmpl).to_string();
                    output.append(Some(res.as_str()))?;
                } else {
                    output.append_null()?;
                }
            }
        }

        Ok(Arc::new((output.finish()?).into()))
    }

    fn eval_row(&self, input: &risingwave_common::array::Row) -> crate::Result<Datum> {
        let data = self.child.eval_row(input)?;
        Ok(if let Some(ScalarImpl::NaiveDateTime(data)) = data {
            Some(data.0.format(&self.ctx.chrono_tmpl).to_string().into())
        } else {
            None
        })
    }
}
