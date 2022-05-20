package com.risingwave.rpc

import com.risingwave.proto.computenode.CreateTaskRequest
import com.risingwave.proto.computenode.CreateTaskResponse
import com.risingwave.proto.computenode.GetDataRequest
import com.risingwave.proto.computenode.GetDataResponse
import com.risingwave.proto.streaming.streamnode.BroadcastActorInfoTableRequest
import com.risingwave.proto.streaming.streamnode.BroadcastActorInfoTableResponse
import com.risingwave.proto.streaming.streamnode.BuildActorsRequest
import com.risingwave.proto.streaming.streamnode.BuildActorsResponse
import com.risingwave.proto.streaming.streamnode.DropActorsRequest
import com.risingwave.proto.streaming.streamnode.DropActorsResponse
import com.risingwave.proto.streaming.streamnode.UpdateActorsRequest
import com.risingwave.proto.streaming.streamnode.UpdateActorsResponse

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

  /** Coroutine-version of UpdateActors. */
  fun UpdateActors(request: UpdateActorsRequest): UpdateActorsResponse

  /** Coroutine-version of BuildActors. */
  fun BuildActors(request: BuildActorsRequest): BuildActorsResponse

  /** Coroutine-version of DropFragment. */
  fun dropFragment(request: DropActorsRequest): DropActorsResponse
}
