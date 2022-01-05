package com.risingwave.rpc

import com.risingwave.common.exception.PgErrorCode
import com.risingwave.common.exception.PgException
import com.risingwave.proto.metanode.CatalogServiceGrpc
import com.risingwave.proto.metanode.CreateMaterializedViewRequest
import com.risingwave.proto.metanode.CreateMaterializedViewResponse
import com.risingwave.proto.metanode.CreateRequest
import com.risingwave.proto.metanode.CreateResponse
import com.risingwave.proto.metanode.DropMaterializedViewRequest
import com.risingwave.proto.metanode.DropMaterializedViewResponse
import com.risingwave.proto.metanode.DropRequest
import com.risingwave.proto.metanode.DropResponse
import com.risingwave.proto.metanode.EpochServiceGrpc
import com.risingwave.proto.metanode.GetCatalogRequest
import com.risingwave.proto.metanode.GetCatalogResponse
import com.risingwave.proto.metanode.GetEpochRequest
import com.risingwave.proto.metanode.GetEpochResponse
import com.risingwave.proto.metanode.GetIdRequest
import com.risingwave.proto.metanode.GetIdResponse
import com.risingwave.proto.metanode.HeartbeatRequest
import com.risingwave.proto.metanode.HeartbeatResponse
import com.risingwave.proto.metanode.HeartbeatServiceGrpc
import com.risingwave.proto.metanode.IdGeneratorServiceGrpc
import com.risingwave.proto.metanode.StreamManagerServiceGrpc
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

  override fun getId(request: GetIdRequest): GetIdResponse {
    val stub = IdGeneratorServiceGrpc.newBlockingStub(channel)
    try {
      return stub.getId(request)
    } catch (e: StatusRuntimeException) {
      LOGGER.warn("RPC failed: {}", e.status)
      throw rpcException("getId", e)
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
}
