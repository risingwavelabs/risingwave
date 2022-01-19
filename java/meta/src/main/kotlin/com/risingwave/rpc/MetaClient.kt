package com.risingwave.rpc

import com.risingwave.proto.metanode.CreateMaterializedViewRequest
import com.risingwave.proto.metanode.CreateMaterializedViewResponse
import com.risingwave.proto.metanode.CreateRequest
import com.risingwave.proto.metanode.CreateResponse
import com.risingwave.proto.metanode.DropMaterializedViewRequest
import com.risingwave.proto.metanode.DropMaterializedViewResponse
import com.risingwave.proto.metanode.DropRequest
import com.risingwave.proto.metanode.DropResponse
import com.risingwave.proto.metanode.GetCatalogRequest
import com.risingwave.proto.metanode.GetCatalogResponse
import com.risingwave.proto.metanode.GetEpochRequest
import com.risingwave.proto.metanode.GetEpochResponse
import com.risingwave.proto.metanode.HeartbeatRequest
import com.risingwave.proto.metanode.HeartbeatResponse

/** A client connecting to meta node. */
interface MetaClient {
  fun getCatalog(request: GetCatalogRequest): GetCatalogResponse

  fun heartbeat(request: HeartbeatRequest): HeartbeatResponse

  fun create(request: CreateRequest): CreateResponse

  fun drop(request: DropRequest): DropResponse

  fun getEpoch(request: GetEpochRequest): GetEpochResponse

  fun createMaterializedView(request: CreateMaterializedViewRequest): CreateMaterializedViewResponse

  fun dropMaterializedView(request: DropMaterializedViewRequest): DropMaterializedViewResponse
}
