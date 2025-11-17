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

use risingwave_common::row::Row;
use risingwave_common::types::ToText;
use risingwave_expr::function;

/// Concatenates all but the first argument, with separators. The first argument is used as the
/// separator string, and should not be NULL. Other NULL arguments are ignored.
///
/// # Example
///
/// ```slt
/// query T
/// select concat_ws(',', 'abcde', 2, NULL, 22);
/// ----
/// abcde,2,22
///
/// query T
/// select concat_ws(',', variadic array['abcde', 2, NULL, 22] :: varchar[]);
/// ----
/// abcde,2,22
/// ```
#[function("concat_ws(varchar, variadic anyarray) -> varchar")]
fn concat_ws(sep: &str, vals: impl Row, writer: &mut impl std::fmt::Write) {
    let mut string_iter = vals.iter().flatten();
    if let Some(string) = string_iter.next() {
        string.write(writer).unwrap();
    }
    for string in string_iter {
        write!(writer, "{}", sep).unwrap();
        string.write(writer).unwrap();
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::array::DataChunk;
    use risingwave_common::row::Row;
    use risingwave_common::test_prelude::DataChunkTestExt;
    use risingwave_common::types::ToOwnedDatum;
    use risingwave_common::util::iter_util::ZipEqDebug;
    use risingwave_expr::expr::build_from_pretty;

    #[tokio::test]
    async fn test_concat_ws() {
        let concat_ws =
            build_from_pretty("(concat_ws:varchar $0:varchar $1:varchar $2:varchar $3:varchar)");
        let (input, expected) = DataChunk::from_pretty(
            "T T T T  T
             , a b c  a,b,c
             , . b c  b,c
             . a b c  .
             , . . .  (empty)
             . . . .  .",
        )
        .split_column_at(4);

        // test eval
        let output = concat_ws.eval(&input).await.unwrap();
        assert_eq!(&output, expected.column_at(0));

        // test eval_row
        for (row, expected) in input.rows().zip_eq_debug(expected.rows()) {
            let result = concat_ws.eval_row(&row.to_owned_row()).await.unwrap();
            assert_eq!(result, expected.datum_at(0).to_owned_datum());
        }
    }
}
