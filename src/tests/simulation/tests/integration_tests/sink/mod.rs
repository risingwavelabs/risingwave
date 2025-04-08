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

#[cfg(madsim)]
mod basic;
#[cfg(madsim)]
mod err_isolation;
#[cfg(madsim)]
mod exactly_once_utils;

#[cfg(madsim)]
mod exactly_once_iceberg;
#[cfg(madsim)]
mod rate_limit;
#[cfg(madsim)]
mod recovery;
#[cfg(madsim)]
mod scale;
#[cfg(madsim)]
mod utils;

#[macro_export]
macro_rules! assert_with_err_returned {
    ($condition:expr, $($rest:tt)*) => {{
        if !$condition {
            return Err(anyhow::anyhow!($($rest)*).into());
        }
    }};
    ($condition:expr) => {{
        if !$condition {
            return Err(anyhow::anyhow!("fail assertion {}", stringify! {$condition}).into());
        }
    }};
}

#[macro_export]
macro_rules! assert_eq_with_err_returned {
    ($first:expr, $second:expr $(,$($rest:tt)*)?) => {{
        $crate::assert_with_err_returned ! {
            {$first == $second}
            $(, $($rest:tt)*)?
        }
    }};
}
