// Copyright 2024 RisingWave Labs
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

use std::sync::Arc;

use anyhow::Context;
use arrow_schema::{Fields, Schema, SchemaRef};
use cfg_or_panic::cfg_or_panic;
use risingwave_common::array::arrow::{FromArrow, ToArrow, UdfArrowConvert};
use risingwave_common::array::I32Array;
use risingwave_common::bail;

use super::*;
use crate::sig::{UdfOptions, UdfRuntime};

#[derive(Debug)]
pub struct UserDefinedTableFunction {
    children: Vec<BoxedExpression>,
    arg_schema: SchemaRef,
    return_type: DataType,
    runtime: Box<dyn UdfRuntime>,
    arrow_convert: UdfArrowConvert,
    #[allow(dead_code)]
    chunk_size: usize,
}

#[async_trait::async_trait]
impl TableFunction for UserDefinedTableFunction {
    fn return_type(&self) -> DataType {
        self.return_type.clone()
    }

    #[cfg_or_panic(not(madsim))]
    async fn eval<'a>(&'a self, input: &'a DataChunk) -> BoxStream<'a, Result<DataChunk>> {
        self.eval_inner(input)
    }
}

#[cfg(not(madsim))]
impl UserDefinedTableFunction {
    #[try_stream(boxed, ok = DataChunk, error = ExprError)]
    async fn eval_inner<'a>(&'a self, input: &'a DataChunk) {
        // evaluate children expressions
        let mut columns = Vec::with_capacity(self.children.len());
        for c in &self.children {
            let val = c.eval(input).await?;
            columns.push(val);
        }
        let direct_input = DataChunk::new(columns, input.visibility().clone());

        // compact the input chunk and record the row mapping
        let visible_rows = direct_input.visibility().iter_ones().collect::<Vec<_>>();
        // this will drop invisible rows
        let arrow_input = self
            .arrow_convert
            .to_record_batch(self.arg_schema.clone(), &direct_input)?;

        // call UDTF
        #[for_await]
        for res in self.runtime.call_table_function(&arrow_input).await? {
            let output = self.arrow_convert.from_record_batch(&res?)?;
            self.check_output(&output)?;

            // we send the compacted input to UDF, so we need to map the row indices back to the
            // original input
            let origin_indices = output
                .column_at(0)
                .as_int32()
                .raw_iter()
                // we have checked all indices are non-negative
                .map(|idx| visible_rows[idx as usize] as i32)
                .collect::<I32Array>();

            let output = DataChunk::new(
                vec![origin_indices.into_ref(), output.column_at(1).clone()],
                output.visibility().clone(),
            );
            yield output;
        }
    }

    /// Check if the output chunk is valid.
    fn check_output(&self, output: &DataChunk) -> Result<()> {
        if output.columns().len() != 2 {
            bail!(
                "UDF returned {} columns, but expected 2",
                output.columns().len()
            );
        }
        if output.column_at(0).data_type() != DataType::Int32 {
            bail!(
                "UDF returned {:?} at column 0, but expected {:?}",
                output.column_at(0).data_type(),
                DataType::Int32,
            );
        }
        if output.column_at(0).as_int32().raw_iter().any(|i| i < 0) {
            bail!("UDF returned negative row index");
        }
        if !output
            .column_at(1)
            .data_type()
            .equals_datatype(&self.return_type)
        {
            bail!(
                "UDF returned {:?} at column 1, but expected {:?}",
                output.column_at(1).data_type(),
                &self.return_type,
            );
        }
        Ok(())
    }
}

#[cfg_or_panic(not(madsim))]
pub fn new_user_defined(prost: &PbTableFunction, chunk_size: usize) -> Result<BoxedTableFunction> {
    let Some(udtf) = &prost.udtf else {
        bail!("expect UDTF");
    };

    let identifier = udtf.get_identifier()?;
    let return_type = DataType::from(prost.get_return_type()?);

    let language = udtf.language.as_str();
    let runtime = udtf.runtime.as_deref();
    let link = udtf.link.as_deref();

    // lookup UDF builder
    let builder = crate::sig::UDF_RUNTIMES
        .iter()
        .find(|udf| (udf.match_)(language, runtime, link))
        .context("language not found")?
        .build;
    let runtime = builder(UdfOptions {
        table_function: true,
        body: udtf.body.as_deref(),
        compressed_binary: udtf.compressed_binary.as_deref(),
        link: udtf.link.as_deref(),
        identifier,
        arg_names: &udtf.arg_names,
        return_type: &return_type,
        always_retry_on_network_error: false,
        function_type: udtf.function_type.as_deref(),
    })
    .context("failed to build UDF runtime")?;

    let arrow_convert = UdfArrowConvert {
        legacy: runtime.is_legacy(),
    };
    let arg_schema = Arc::new(Schema::new(
        udtf.arg_types
            .iter()
            .map(|t| arrow_convert.to_arrow_field("", &DataType::from(t)))
            .try_collect::<Fields>()?,
    ));

    Ok(UserDefinedTableFunction {
        children: prost.args.iter().map(expr_build_from_prost).try_collect()?,
        return_type,
        arg_schema,
        runtime,
        arrow_convert,
        chunk_size,
    }
    .boxed())
}
