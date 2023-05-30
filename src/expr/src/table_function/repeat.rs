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

use super::*;

/// Repeat an expression n times.
///
/// Should only be used for tests now.
pub fn repeat(expr: BoxedExpression, n: usize) -> BoxedTableFunction {
    RepeatN { expr, n }.boxed()
}

#[derive(Debug)]
struct RepeatN {
    expr: BoxedExpression,
    n: usize,
}

#[async_trait::async_trait]
impl TableFunction for RepeatN {
    fn return_type(&self) -> DataType {
        self.expr.return_type()
    }

    async fn eval<'a>(&'a self, input: &'a DataChunk) -> BoxStream<'a, Result<DataChunk>> {
        self.eval_inner(input)
    }
}

impl RepeatN {
    #[try_stream(boxed, ok = DataChunk, error = ExprError)]
    async fn eval_inner<'a>(&'a self, input: &'a DataChunk) {
        let array = self.expr.eval(input).await?;

        let mut index_builder = I64ArrayBuilder::new(0x100);
        let mut value_builder = self.return_type().create_array_builder(0x100);
        for (i, value) in array.iter().enumerate() {
            index_builder.append_n(self.n, Some(i as i64));
            value_builder.append_n(self.n, value);
        }
        let len = index_builder.len();
        let index_array: ArrayImpl = index_builder.finish().into();
        let value_array = value_builder.finish();
        yield DataChunk::new(vec![index_array.into(), value_array.into()], len);
    }
}
