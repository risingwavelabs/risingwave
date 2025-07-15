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

use std::sync::Arc;

use risingwave_connector::sink::catalog::SinkCatalog;
use risingwave_sqlparser::ast::ObjectName;

use crate::Binder;
use crate::binder::BindFor;
use crate::catalog::root_catalog::SchemaPath;
use crate::catalog::view_catalog::ViewCatalog;
use crate::error::Result;

pub struct BoundSink {
    pub sink_catalog: Arc<SinkCatalog>,
}

pub struct BoundView {
    pub view_catalog: Arc<ViewCatalog>,
}

impl Binder {
    pub fn bind_sink_by_name(&self, name: ObjectName) -> Result<BoundSink> {
        matches!(self.bind_for, BindFor::System);
        let (schema_name, sink_name) = Self::resolve_schema_qualified_name(&self.db_name, name)?;

        let search_path = SchemaPath::new(
            schema_name.as_deref(),
            &self.search_path,
            &self.auth_context.user_name,
        );
        let (sink_catalog, _) =
            self.catalog
                .get_any_sink_by_name(&self.db_name, search_path, &sink_name)?;
        Ok(BoundSink {
            sink_catalog: sink_catalog.clone(),
        })
    }

    pub fn bind_view_by_name(&self, name: ObjectName) -> Result<BoundView> {
        matches!(self.bind_for, BindFor::System);
        let (schema_name, view_name) = Self::resolve_schema_qualified_name(&self.db_name, name)?;

        let search_path = SchemaPath::new(
            schema_name.as_deref(),
            &self.search_path,
            &self.auth_context.user_name,
        );
        let (view_catalog, _) =
            self.catalog
                .get_view_by_name(&self.db_name, search_path, &view_name)?;
        Ok(BoundView {
            view_catalog: view_catalog.clone(),
        })
    }
}
