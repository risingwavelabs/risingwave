package com.risingwave.rpc

import com.risingwave.proto.metadatanode.CreateRequest
import com.risingwave.proto.metadatanode.CreateResponse
import com.risingwave.proto.metadatanode.DropRequest
import com.risingwave.proto.metadatanode.DropResponse
import com.risingwave.proto.metadatanode.GetCatalogRequest
import com.risingwave.proto.metadatanode.GetCatalogResponse
import com.risingwave.proto.metadatanode.GetEpochRequest
import com.risingwave.proto.metadatanode.GetEpochResponse
import com.risingwave.proto.metadatanode.GetIdRequest
import com.risingwave.proto.metadatanode.GetIdResponse
import com.risingwave.proto.metadatanode.HeartbeatRequest
import com.risingwave.proto.metadatanode.HeartbeatResponse

/** A client connecting to MetadataNode. */
interface MetaClient {
  fun getCatalog(request: GetCatalogRequest): GetCatalogResponse

  fun heartbeat(request: HeartbeatRequest): HeartbeatResponse

  fun create(request: CreateRequest): CreateResponse

  fun drop(request: DropRequest): DropResponse

  fun getEpoch(request: GetEpochRequest): GetEpochResponse

  fun getId(request: GetIdRequest): GetIdResponse
}
