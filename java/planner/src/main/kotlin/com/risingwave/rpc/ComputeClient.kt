package com.risingwave.rpc

import com.risingwave.proto.computenode.CreateTaskRequest
import com.risingwave.proto.computenode.CreateTaskResponse
import com.risingwave.proto.computenode.GetDataRequest
import com.risingwave.proto.computenode.GetDataResponse
import com.risingwave.proto.streaming.streamnode.BroadcastActorInfoTableRequest
import com.risingwave.proto.streaming.streamnode.BroadcastActorInfoTableResponse
import com.risingwave.proto.streaming.streamnode.BuildFragmentRequest
import com.risingwave.proto.streaming.streamnode.BuildFragmentResponse
import com.risingwave.proto.streaming.streamnode.DropFragmentsRequest
import com.risingwave.proto.streaming.streamnode.DropFragmentsResponse
import com.risingwave.proto.streaming.streamnode.UpdateFragmentRequest
import com.risingwave.proto.streaming.streamnode.UpdateFragmentResponse

/** A client connecting to ComputeNodes. */
interface ComputeClient {
  /** Coroutine-version of createTask. */
  suspend fun createTaskKt(request: CreateTaskRequest): CreateTaskResponse

  /** Blocking-version of createTask. */
  @Deprecated(message = "Use createTaskKt to prevent blocking the thread")
  fun createTask(request: CreateTaskRequest): CreateTaskResponse

  fun getData(request: GetDataRequest): Iterator<GetDataResponse>

  /** Coroutine-version of BroadcastActorInfoTable. */
  fun broadcastActorInfoTable(request: BroadcastActorInfoTableRequest): BroadcastActorInfoTableResponse

  /** Coroutine-version of UpdateFragment. */
  fun updateFragment(request: UpdateFragmentRequest): UpdateFragmentResponse

  /** Coroutine-version of BuildFragment. */
  fun buildFragment(request: BuildFragmentRequest): BuildFragmentResponse

  /** Coroutine-version of DropFragment. */
  fun dropFragment(request: DropFragmentsRequest): DropFragmentsResponse
}
