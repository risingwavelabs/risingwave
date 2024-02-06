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

use risingwave_common::error::v2::def_anyhow_newtype;

def_anyhow_newtype! {
    /// The error type for the `connector` crate.
    ///
    /// We use [`anyhow::Error`] under the hood as the connector has to deal with
    /// various kinds of errors from different external crates. It acts more like an
    /// application and callers may not expect to handle it in a fine-grained way.
    pub ConnectorError;
}

pub type ConnectorResult<T> = std::result::Result<T, ConnectorError>;
