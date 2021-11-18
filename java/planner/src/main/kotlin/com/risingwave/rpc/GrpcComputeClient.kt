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
import com.risingwave.proto.streaming.streamnode.BroadcastActorInfoTableRequest
import com.risingwave.proto.streaming.streamnode.BroadcastActorInfoTableResponse
import com.risingwave.proto.streaming.streamnode.BuildFragmentRequest
import com.risingwave.proto.streaming.streamnode.BuildFragmentResponse
import com.risingwave.proto.streaming.streamnode.DropFragmentsRequest
import com.risingwave.proto.streaming.streamnode.DropFragmentsResponse
import com.risingwave.proto.streaming.streamnode.StreamServiceGrpc
import com.risingwave.proto.streaming.streamnode.UpdateFragmentRequest
import com.risingwave.proto.streaming.streamnode.UpdateFragmentResponse
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

  override fun broadcastActorInfoTable(request: BroadcastActorInfoTableRequest): BroadcastActorInfoTableResponse {
    val stub = StreamServiceGrpc.newBlockingStub(channel)
    try {
      return stub.broadcastActorInfoTable(request)
    } catch (e: StatusRuntimeException) {
      LOGGER.warn("RPC failed: {}", e.status)
      throw rpcException("createTaskKt", e)
    }
  }

  override fun updateFragment(request: UpdateFragmentRequest): UpdateFragmentResponse {
    val stub = StreamServiceGrpc.newBlockingStub(channel)
    try {
      return stub.updateFragment(request)
    } catch (e: StatusRuntimeException) {
      LOGGER.warn("RPC failed: {}", e.status)
      throw rpcException("createTaskKt", e)
    }
  }

  override fun buildFragment(request: BuildFragmentRequest): BuildFragmentResponse {
    val stub = StreamServiceGrpc.newBlockingStub(channel)
    try {
      return stub.buildFragment(request)
    } catch (e: StatusRuntimeException) {
      LOGGER.warn("RPC failed: {}", e.status)
      throw rpcException("createTaskKt", e)
    }
  }

  override fun dropFragment(request: DropFragmentsRequest): DropFragmentsResponse {
    val stub = StreamServiceGrpc.newBlockingStub(channel)
    try {
      return stub.dropFragment(request)
    } catch (e: StatusRuntimeException) {
      LOGGER.warn("RPC failed: {}", e.status)
      throw rpcException("createTaskKt", e)
    }
  }
}
