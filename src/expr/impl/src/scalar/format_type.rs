// Copyright 2025 RisingWave Labs
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

use risingwave_common::types::DataType;
use risingwave_expr::function;

#[function("format_type(int4, int4) -> varchar")]
pub fn format_type(oid: i32, _typemod: Option<i32>, writer: &mut impl std::fmt::Write) {
    // since we don't support type modifier, ignore it.
    match DataType::from_oid(oid) {
        Ok(dt) => write!(writer, "{}", dt).unwrap(),
        Err(_) => write!(writer, "???").unwrap(),
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::array::DataChunk;
    use risingwave_common::test_prelude::DataChunkTestExt;
    use risingwave_expr::expr::build_from_pretty;

    #[tokio::test]
    async fn test_format_type() {
        let (input, target) = DataChunk::from_pretty(
            "
            i       i T
            16      0 boolean
            21      . smallint
            9527    0 ???
            .       0 .
            ",
        )
        .split_column_at(2);
        let expr = build_from_pretty("(format_type:varchar $0:int4 $1:int4)");
        let result = expr.eval(&input).await.unwrap();
        assert_eq!(&result, target.column_at(0));
    }
}
