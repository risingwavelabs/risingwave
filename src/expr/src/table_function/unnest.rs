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

use risingwave_common::array::{Array, DataChunk, ListArray};
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
use risingwave_common::util::iter_util::ZipEqFast;

use super::*;

#[derive(Debug)]
pub struct Unnest {
    return_type: DataType,
    list: BoxedExpression,
    chunk_size: usize,
}

impl Unnest {
    #[try_stream(boxed, ok = DataChunk, error = ExprError)]
    async fn eval_inner<'a>(&'a self, input: &'a DataChunk) {
        let ret_list = self.list.eval_checked(input).await?;
        let arr_list: &ListArray = ret_list.as_ref().into();

        let mut builder =
            DataChunkBuilder::new(vec![DataType::Int64, self.return_type()], self.chunk_size);
        for (i, (list, visible)) in arr_list.iter().zip_eq_fast(input.vis().iter()).enumerate() {
            if let Some(list) = list && visible {
                for value in list.flatten() {
                    if let Some(chunk) = builder.append_one_row([Some((i as i64).into()), value]) {
                        yield chunk;
                    }
                }
            }
        }
        if let Some(chunk) = builder.consume_all() {
            yield chunk;
        }
    }
}

#[async_trait::async_trait]
impl TableFunction for Unnest {
    fn return_type(&self) -> DataType {
        self.return_type.clone()
    }

    async fn eval<'a>(&'a self, input: &'a DataChunk) -> BoxStream<'a, Result<DataChunk>> {
        self.eval_inner(input)
    }
}

pub fn new_unnest(prost: &PbTableFunction, chunk_size: usize) -> Result<BoxedTableFunction> {
    let return_type = DataType::from(prost.get_return_type().unwrap());
    let args: Vec<_> = prost.args.iter().map(expr_build_from_prost).try_collect()?;
    let [list]: [_; 1] = args.try_into().unwrap();

    Ok(Unnest {
        return_type,
        list,
        chunk_size,
    }
    .boxed())
}
