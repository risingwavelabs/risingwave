// Copyright 2026 RisingWave Labs
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

//! RisingWave scalar function wrapper for DataFusion.
//!
//! This module provides [`RwScalarFunction`], which implements DataFusion's [`ScalarUDFImpl`]
//! trait to wrap RisingWave's expression evaluation logic. This allows RisingWave functions
//! that cannot be directly converted to DataFusion expressions to be executed within
//! DataFusion's query execution engine.

use datafusion::arrow::datatypes::DataType as DFDataType;
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature};
use datafusion_common::Result as DFResult;
use risingwave_common::array::arrow::IcebergArrowConvert;
use risingwave_expr::expr::{BoxedExpression, ValueImpl};

use crate::datafusion::{
    CastExecutor, convert_scalar_value, create_data_chunk, to_datafusion_error,
};
use crate::error::RwError;

/// A wrapper that allows RisingWave scalar functions to be executed within DataFusion.
///
/// This struct implements [`ScalarUDFImpl`] and handles:
/// - Type casting from DataFusion types to RisingWave types
/// - Data chunk creation and manipulation
/// - Async expression evaluation using RisingWave's executor
/// - Result conversion back to DataFusion-compatible types
#[derive(Debug, educe::Educe)]
#[educe(PartialEq, Eq, Hash)]
pub struct RwScalarFunction {
    /// DataFusion uses function name as column identifier, so we need to keep unique names
    /// for different functions to avoid conflicts
    pub(super) name: String,
    pub(super) column_name: Vec<String>,
    #[educe(PartialEq(ignore), Hash(ignore))]
    pub(super) cast: CastExecutor,
    #[educe(PartialEq(ignore), Hash(ignore))]
    pub(super) expr: BoxedExpression,
    #[educe(PartialEq(ignore), Hash(ignore))]
    pub(super) signature: Signature,
}

impl ScalarUDFImpl for RwScalarFunction {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DFDataType]) -> DFResult<DFDataType> {
        let field = IcebergArrowConvert
            .to_arrow_field("", &self.expr.return_type())
            .map_err(to_datafusion_error)?;
        Ok(field.data_type().clone())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let arrays = args
            .args
            .into_iter()
            .map(|cv| cv.into_array(1))
            .collect::<DFResult<Vec<_>>>()?;
        let chunk =
            create_data_chunk(arrays.into_iter(), args.number_rows).map_err(to_datafusion_error)?;

        let value = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let chunk = self.cast.execute(chunk).await?;
                let value = self.expr.eval_v2(&chunk).await?;
                Ok::<ValueImpl, RwError>(value)
            })
        })
        .map_err(to_datafusion_error)?;

        let res = match value {
            ValueImpl::Array(array_impl) => {
                let array = IcebergArrowConvert
                    .to_arrow_array(args.return_field.data_type(), &array_impl)
                    .map_err(to_datafusion_error)?;
                ColumnarValue::Array(array)
            }
            ValueImpl::Scalar { value, .. } => {
                let value = convert_scalar_value(&value, self.expr.return_type())
                    .map_err(to_datafusion_error)?;
                ColumnarValue::Scalar(value)
            }
        };
        Ok(res)
    }
}
