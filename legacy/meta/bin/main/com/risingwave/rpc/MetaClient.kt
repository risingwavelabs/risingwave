package com.risingwave.rpc

import com.risingwave.proto.hummock.*
import com.risingwave.proto.metanode.*

/** A client connecting to meta node. */
interface MetaClient {
  fun getCatalog(request: GetCatalogRequest): GetCatalogResponse

  fun heartbeat(request: HeartbeatRequest): HeartbeatResponse

  fun create(request: CreateRequest): CreateResponse

  fun drop(request: DropRequest): DropResponse

  fun getEpoch(request: GetEpochRequest): GetEpochResponse

  fun createMaterializedView(request: CreateMaterializedViewRequest): CreateMaterializedViewResponse

  fun dropMaterializedView(request: DropMaterializedViewRequest): DropMaterializedViewResponse

  fun flush(request: FlushRequest): FlushResponse

  fun listAllNodes(request: ListAllNodesRequest): ListAllNodesResponse

  fun pinSnapshot(request: PinSnapshotRequest): PinSnapshotResponse

  fun unpinSnapshot(request: UnpinSnapshotRequest): UnpinSnapshotResponse
}
