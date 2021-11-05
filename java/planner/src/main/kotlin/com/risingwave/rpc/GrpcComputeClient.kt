package com.risingwave.rpc

import com.risingwave.common.exception.PgErrorCode
import com.risingwave.common.exception.PgException
import com.risingwave.proto.computenode.CreateTaskRequest
import com.risingwave.proto.computenode.CreateTaskResponse
import com.risingwave.proto.computenode.ExchangeServiceGrpc
import com.risingwave.proto.computenode.GetDataRequest
import com.risingwave.proto.computenode.GetDataResponse
import com.risingwave.proto.computenode.TaskServiceGrpc
import com.risingwave.proto.computenode.TaskServiceGrpcKt
import io.grpc.Channel
import io.grpc.StatusRuntimeException
import org.slf4j.LoggerFactory

/** A ComputeClient implementation based on grpc. */
class GrpcComputeClient(private val channel: Channel) : ComputeClient {
  companion object {
    private val LOGGER = LoggerFactory.getLogger(GrpcComputeClient::class.java)

    private fun rpcException(rpcName: String, e: StatusRuntimeException): PgException {
      throw PgException(PgErrorCode.INTERNAL_ERROR, "%s RPC failed: %s", rpcName, e.toString())
    }
  }

  override fun createTask(request: CreateTaskRequest): CreateTaskResponse {
    val stub = TaskServiceGrpc.newBlockingStub(channel)
    try {
      return stub.createTask(request)
    } catch (e: StatusRuntimeException) {
      LOGGER.warn("RPC failed: {}", e.status)
      throw rpcException("createTask", e)
    }
  }

  override suspend fun createTaskKt(request: CreateTaskRequest): CreateTaskResponse {
    val stub = TaskServiceGrpcKt.TaskServiceCoroutineStub(channel)
    try {
      return stub.createTask(request)
    } catch (e: StatusRuntimeException) {
      LOGGER.warn("RPC failed: {}", e.status)
      throw rpcException("createTaskKt", e)
    }
  }

  override fun getData(request: GetDataRequest): Iterator<GetDataResponse> {
    // Prepare Exchange service stub.
    val stub = ExchangeServiceGrpc.newBlockingStub(channel)
    try {
      return stub.getData(request)
    } catch (e: StatusRuntimeException) {
      LOGGER.warn("RPC failed: {}", e.status)
      throw rpcException("getData", e)
    }
  }
}
