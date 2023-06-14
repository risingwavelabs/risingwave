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

use risingwave_pb::meta::serving_service_server::ServingService;
use risingwave_pb::meta::{
    FragmentParallelUnitMapping, GetServingVnodeMappingsRequest, GetServingVnodeMappingsResponse,
};
use tonic::{Request, Response, Status};

use crate::serving::ServingVnodeMappingRef;

pub struct ServingServiceImpl {
    serving_vnode_mapping: ServingVnodeMappingRef,
}

impl ServingServiceImpl {
    pub fn new(serving_vnode_mapping: ServingVnodeMappingRef) -> Self {
        Self {
            serving_vnode_mapping,
        }
    }
}

#[async_trait::async_trait]
impl ServingService for ServingServiceImpl {
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
        Ok(Response::new(GetServingVnodeMappingsResponse { mappings }))
    }
}
