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

/// An empty table function that returns nothing.
pub fn empty(return_type: DataType) -> BoxedTableFunction {
    Empty { return_type }.boxed()
}

#[derive(Debug)]
struct Empty {
    return_type: DataType,
}

#[async_trait::async_trait]
impl TableFunction for Empty {
    fn return_type(&self) -> DataType {
        self.return_type.clone()
    }

    async fn eval<'a>(&'a self, _input: &'a DataChunk) -> BoxStream<'a, Result<DataChunk>> {
        futures_util::stream::empty().boxed()
    }
}
