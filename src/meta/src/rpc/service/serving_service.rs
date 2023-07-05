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

use itertools::Itertools;
use risingwave_pb::meta::serving_service_server::ServingService;
use risingwave_pb::meta::{
    FragmentParallelUnitMapping, GetServingVnodeMappingsRequest, GetServingVnodeMappingsResponse,
};
use tonic::{Request, Response, Status};

use crate::manager::FragmentManagerRef;
use crate::serving::ServingVnodeMappingRef;
use crate::storage::MetaStore;

pub struct ServingServiceImpl<S: MetaStore> {
    serving_vnode_mapping: ServingVnodeMappingRef,
    fragment_manager: FragmentManagerRef<S>,
}

impl<S> ServingServiceImpl<S>
where
    S: MetaStore,
{
    pub fn new(
        serving_vnode_mapping: ServingVnodeMappingRef,
        fragment_manager: FragmentManagerRef<S>,
    ) -> Self {
        Self {
            serving_vnode_mapping,
            fragment_manager,
        }
    }
}

#[async_trait::async_trait]
impl<S> ServingService for ServingServiceImpl<S>
where
    S: MetaStore,
{
    async fn get_serving_vnode_mappings(
        &self,
        _request: Request<GetServingVnodeMappingsRequest>,
    ) -> Result<Response<GetServingVnodeMappingsResponse>, Status> {
        let mappings = self
            .serving_vnode_mapping
            .all()
            .into_iter()
            .map(|(fragment_id, mapping)| FragmentParallelUnitMapping {
                fragment_id,
                mapping: Some(mapping.to_protobuf()),
            })
            .collect();
        let fragment_to_table = {
            let guard = self.fragment_manager.get_fragment_read_guard().await;
            guard
                .table_fragments()
                .iter()
                .flat_map(|(table_id, tf)| {
                    tf.fragment_ids()
                        .map(|fragment_id| (fragment_id, table_id.table_id))
                        .collect_vec()
                })
                .collect()
        };
        Ok(Response::new(GetServingVnodeMappingsResponse {
            mappings,
            fragment_to_table,
        }))
    }
}
