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

use std::sync::Arc;

use risingwave_common::session_config::SearchPath;
use risingwave_expr::define_context;

use crate::session::AuthContext;

// Only for local mode.
define_context! {
    pub(super) CATALOG_READER: crate::catalog::CatalogReader,
    pub(super) AUTH_CONTEXT: Arc<AuthContext>,
    pub(super) DB_NAME: String,
    pub(super) SEARCH_PATH: SearchPath,
}
