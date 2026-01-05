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

use crate::util::meta_addr::MetaAddressStrategy;

/// Accessor trait for a component's command-line options.
pub trait Opts {
    /// The name of the component.
    fn name() -> &'static str;

    /// The address to the meta node.
    fn meta_addr(&self) -> MetaAddressStrategy;
}
