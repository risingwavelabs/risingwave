package com.risingwave.rpc

import com.risingwave.common.exception.PgErrorCode
import com.risingwave.common.exception.PgException
import com.risingwave.proto.metanode.*
import io.grpc.Channel
import io.grpc.StatusRuntimeException
import org.slf4j.LoggerFactory

/** A MetaClient implementation based on grpc. */
class GrpcMetaClient(private val channel: Channel) : MetaClient {
  companion object {
    private val LOGGER = LoggerFactory.getLogger(GrpcMetaClient::class.java)

    private fun rpcException(rpcName: String, e: StatusRuntimeException): PgException {
      throw PgException(PgErrorCode.INTERNAL_ERROR, "%s RPC failed: %s", rpcName, e.toString())
    }
  }

  override fun getCatalog(request: GetCatalogRequest): GetCatalogResponse {
    val stub = CatalogServiceGrpc.newBlockingStub(channel)
    try {
      return stub.getCatalog(request)
    } catch (e: StatusRuntimeException) {
      LOGGER.warn("RPC failed: {}", e.status)
      throw rpcException("getCatalog", e)
    }
  }

  override fun heartbeat(request: HeartbeatRequest): HeartbeatResponse {
    val stub = HeartbeatServiceGrpc.newBlockingStub(channel)
    try {
      return stub.heartbeat(request)
    } catch (e: StatusRuntimeException) {
      LOGGER.warn("RPC failed: {}", e.status)
      throw rpcException("heartbeat", e)
    }
  }

  override fun create(request: CreateRequest): CreateResponse {
    val stub = CatalogServiceGrpc.newBlockingStub(channel)
    try {
      return stub.create(request)
    } catch (e: StatusRuntimeException) {
      LOGGER.warn("RPC failed: {}", e.status)
      throw rpcException("create", e)
    }
  }

  override fun drop(request: DropRequest): DropResponse {
    val stub = CatalogServiceGrpc.newBlockingStub(channel)
    try {
      return stub.drop(request)
    } catch (e: StatusRuntimeException) {
      LOGGER.warn("RPC failed: {}", e.status)
      throw rpcException("drop", e)
    }
  }

  override fun getEpoch(request: GetEpochRequest): GetEpochResponse {
    val stub = EpochServiceGrpc.newBlockingStub(channel)
    try {
      return stub.getEpoch(request)
    } catch (e: StatusRuntimeException) {
      LOGGER.warn("RPC failed: {}", e.status)
      throw rpcException("getEpoch", e)
    }
  }

  override fun createMaterializedView(request: CreateMaterializedViewRequest): CreateMaterializedViewResponse {
    val stub = StreamManagerServiceGrpc.newBlockingStub(channel)
    try {
      return stub.createMaterializedView(request)
    } catch (e: StatusRuntimeException) {
      LOGGER.warn("RPC failed: {}", e.status)
      throw rpcException("createMaterializedView", e)
    }
  }

  override fun dropMaterializedView(request: DropMaterializedViewRequest): DropMaterializedViewResponse {
    val stub = StreamManagerServiceGrpc.newBlockingStub(channel)
    try {
      return stub.dropMaterializedView(request)
    } catch (e: StatusRuntimeException) {
      LOGGER.warn("RPC failed: {}", e.status)
      throw rpcException("dropMaterializedView", e)
    }
  }

  override fun flush(request: FlushRequest): FlushResponse {
    val stub = StreamManagerServiceGrpc.newBlockingStub(channel)
    try {
      return stub.flush(request)
    } catch (e: StatusRuntimeException) {
      LOGGER.warn("RPC failed: {}", e.status)
      throw rpcException("flush", e)
    }
  }

  override fun listAllNodes(request: ListAllNodesRequest): ListAllNodesResponse {
    val stub = ClusterServiceGrpc.newBlockingStub(channel)
    try {
      return stub.listAllNodes(request)
    } catch (e: StatusRuntimeException) {
      LOGGER.warn("RPC failed: {}", e.status)
      throw rpcException("listAllNodes", e)
    }
  }
}
