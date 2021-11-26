package com.risingwave.rpc

import com.risingwave.common.exception.PgErrorCode
import com.risingwave.common.exception.PgException
import com.risingwave.proto.metadatanode.CatalogServiceGrpc
import com.risingwave.proto.metadatanode.CreateRequest
import com.risingwave.proto.metadatanode.CreateResponse
import com.risingwave.proto.metadatanode.DropRequest
import com.risingwave.proto.metadatanode.DropResponse
import com.risingwave.proto.metadatanode.EpochServiceGrpc
import com.risingwave.proto.metadatanode.GetCatalogRequest
import com.risingwave.proto.metadatanode.GetCatalogResponse
import com.risingwave.proto.metadatanode.GetEpochRequest
import com.risingwave.proto.metadatanode.GetEpochResponse
import com.risingwave.proto.metadatanode.GetIdRequest
import com.risingwave.proto.metadatanode.GetIdResponse
import com.risingwave.proto.metadatanode.HeartbeatRequest
import com.risingwave.proto.metadatanode.HeartbeatResponse
import com.risingwave.proto.metadatanode.HeartbeatServiceGrpc
import com.risingwave.proto.metadatanode.IdGeneratorServiceGrpc
import io.grpc.Channel
import io.grpc.StatusRuntimeException
import org.slf4j.LoggerFactory

/** A MetadataClient implementation based on grpc. */
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
}
