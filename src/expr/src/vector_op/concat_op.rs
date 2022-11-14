// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use risingwave_common::array::{BytesGuard, BytesWriter};

use crate::Result;

#[inline(always)]
pub fn concat_op(left: &str, right: &str, dst: BytesWriter) -> Result<BytesGuard> {
    let mut writer = dst.begin();
    writer.write_ref(left)?;
    writer.write_ref(right)?;
    writer.finish().map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use risingwave_common::array::{Array, ArrayBuilder, Utf8ArrayBuilder};

    use super::*;

    #[test]
    fn test_concat_op() {
        let writer = Utf8ArrayBuilder::new(1).writer();
        let guard = concat_op("114", "514", writer).unwrap();
        let array = guard.into_inner().finish();

        assert_eq!(array.value_at(0).unwrap(), "114514".to_owned())
    }
}
