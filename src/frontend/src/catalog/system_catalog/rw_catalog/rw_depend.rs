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

use risingwave_common::types::Fields;
use risingwave_frontend_macro::system_catalog;

use crate::catalog::system_catalog::SysCatalogReaderImpl;
use crate::error::Result;

/// The catalog `rw_depend` records the dependency relationships between tables, mviews etc.
#[derive(Fields)]
#[primary_key(objid, refobjid)]
struct RwDepend {
    /// The OID of the specific dependent object
    objid: i32,
    /// The OID of the specific referenced object
    refobjid: i32,
}

#[system_catalog(table, "rw_catalog.rw_depend")]
async fn read_rw_depend(reader: &SysCatalogReaderImpl) -> Result<Vec<RwDepend>> {
    let dependencies = reader.meta_client.list_object_dependencies().await?;
    Ok(dependencies
        .into_iter()
        .map(|depend| RwDepend {
            objid: depend.object_id.as_i32_id(),
            refobjid: depend.referenced_object_id.as_i32_id(),
        })
        .collect())
}
