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

use risingwave_expr::aggregate;

#[aggregate("string_agg(varchar, varchar) -> varchar")]
fn string_agg(
    state: Option<Box<str>>,
    value: Option<&str>,
    delimiter: Option<&str>,
) -> Option<Box<str>> {
    let Some(value) = value else { return state };
    let Some(state) = state else {
        return Some(value.into());
    };
    let mut state = String::from(state);
    state += delimiter.unwrap_or("");
    state += value;
    Some(state.into())
}

#[cfg(test)]
mod tests {
    use risingwave_common::array::*;
    use risingwave_expr::Result;
    use risingwave_expr::aggregate::{AggCall, build_append_only};

    #[tokio::test]
    async fn test_string_agg_basic() -> Result<()> {
        let chunk = StreamChunk::from_pretty(
            " T   T
            + aaa ,
            + bbb ,
            + ccc ,
            + ddd ,",
        );
        let string_agg = build_append_only(&AggCall::from_pretty(
            "(string_agg:varchar $0:varchar $1:varchar)",
        ))?;
        let mut state = string_agg.create_state()?;
        string_agg.update(&mut state, &chunk).await?;
        assert_eq!(
            string_agg.get_result(&state).await?,
            Some("aaa,bbb,ccc,ddd".into())
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_string_agg_complex() -> Result<()> {
        let chunk = StreamChunk::from_pretty(
            " T   T
            + aaa ,
            + .   _
            + ccc _
            + ddd .",
        );
        let string_agg = build_append_only(&AggCall::from_pretty(
            "(string_agg:varchar $0:varchar $1:varchar)",
        ))?;
        let mut state = string_agg.create_state()?;
        string_agg.update(&mut state, &chunk).await?;
        assert_eq!(
            string_agg.get_result(&state).await?,
            Some("aaa_cccddd".into())
        );
        Ok(())
    }
}
