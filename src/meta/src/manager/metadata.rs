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

use crate::controller::catalog::CatalogControllerRef;
use crate::controller::cluster::ClusterControllerRef;
use crate::manager::{CatalogManagerRef, ClusterManagerRef, FragmentManagerRef};

#[derive(Clone)]
pub enum MetadataFucker {
    V1(MetadataFuckerV1),
    V2(MetadataFuckerV2),
}

#[derive(Clone)]
pub struct MetadataFuckerV1 {
    pub cluster_manager: ClusterManagerRef,
    pub catalog_manager: CatalogManagerRef,
    pub fragment_manager: FragmentManagerRef,
}

#[derive(Clone)]
pub struct MetadataFuckerV2 {
    pub cluster_controller: ClusterControllerRef,
    pub catalog_controller: CatalogControllerRef,
}

impl MetadataFucker {
    pub fn new_v1(
        cluster_manager: ClusterManagerRef,
        catalog_manager: CatalogManagerRef,
        fragment_manager: FragmentManagerRef,
    ) -> Self {
        Self::V1(MetadataFuckerV1 {
            cluster_manager,
            catalog_manager,
            fragment_manager,
        })
    }

    pub fn new_v2(
        cluster_controller: ClusterControllerRef,
        catalog_controller: CatalogControllerRef,
    ) -> Self {
        Self::V2(MetadataFuckerV2 {
            cluster_controller,
            catalog_controller,
        })
    }
}
