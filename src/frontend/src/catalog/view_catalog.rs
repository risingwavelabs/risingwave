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

use risingwave_pb::catalog::View as ProstView;

use super::ViewId;
use crate::WithOptions;

#[derive(Clone, Debug)]
pub struct ViewCatalog {
    pub id: ViewId,
    pub name: String,

    pub owner: u32,
    pub properties: WithOptions,
}

impl From<&ProstView> for ViewCatalog {
    fn from(view: &ProstView) -> Self {
        ViewCatalog {
            id: view.id,
            name: view.name.clone(),
            owner: view.owner,
            properties: WithOptions::new(view.properties.clone()),
        }
    }
}
