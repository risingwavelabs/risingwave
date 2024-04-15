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

use super::reader::SystemParamsRead;
use crate::for_all_params;

macro_rules! define_diff {
    ($({ $field:ident, $type:ty, $default:expr, $is_mutable:expr, $doc:literal, $($rest:tt)* },)*) => {
        /// The diff of the system params.
        ///
        /// Fields that are changed are set to `Some`, otherwise `None`.
        #[derive(Default, Debug, Clone)]
        pub struct SystemParamsDiff {
            $(
                #[doc = $doc]
                pub $field: Option<$type>,
            )*
        }
    }
}
for_all_params!(define_diff);

impl SystemParamsDiff {
    /// Create a diff between the given two system params.
    pub fn diff(prev: impl SystemParamsRead, curr: impl SystemParamsRead) -> Self {
        let mut diff = Self::default();

        macro_rules! set_diff_field {
            ($({ $field:ident, $($rest:tt)* },)*) => {
                $(
                    if curr.$field() != prev.$field() {
                        diff.$field = Some(curr.$field().to_owned());
                    }
                )*
            };
        }
        for_all_params!(set_diff_field);

        diff
    }

    /// Create a diff from the given initial system params.
    /// All fields will be set to `Some`.
    pub fn from_initial(initial: impl SystemParamsRead) -> Self {
        macro_rules! initial_field {
            ($({ $field:ident, $($rest:tt)* },)*) => {
                Self {
                    $(
                        $field: Some(initial.$field().to_owned()),
                    )*
                }
            };
        }
        for_all_params!(initial_field)
    }
}
