package com.risingwave.rpc

import com.risingwave.proto.computenode.CreateTaskRequest
import com.risingwave.proto.computenode.CreateTaskResponse
import com.risingwave.proto.computenode.GetDataRequest
import com.risingwave.proto.computenode.GetDataResponse

/** A client connecting to ComputeNodes. */
interface ComputeClient {
  /** Coroutine-version of createTask. */
  suspend fun createTaskKt(request: CreateTaskRequest): CreateTaskResponse

  /** Blocking-version of createTask. */
  @Deprecated(message = "Use createTaskKt to prevent blocking the thread")
  fun createTask(request: CreateTaskRequest): CreateTaskResponse

  fun getData(request: GetDataRequest): Iterator<GetDataResponse>
}
