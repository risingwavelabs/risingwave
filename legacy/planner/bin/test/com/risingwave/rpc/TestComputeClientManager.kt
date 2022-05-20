package com.risingwave.rpc

import com.risingwave.node.WorkerNode
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

class TestComputeClientManager : ComputeClientManager {
  class TestClient : ComputeClient {
    override fun createTask(request: CreateTaskRequest): CreateTaskResponse {
      return CreateTaskResponse.newBuilder().build()
    }

    override fun getData(request: GetDataRequest): Iterator<GetDataResponse> {
      val responses = ArrayList<GetDataResponse>()
      return responses.iterator()
    }

    override fun broadcastActorInfoTable(request: BroadcastActorInfoTableRequest): BroadcastActorInfoTableResponse {
      return BroadcastActorInfoTableResponse.newBuilder().build()
    }

    override fun UpdateActors(request: UpdateActorsRequest): UpdateActorsResponse {
      return UpdateActorsResponse.newBuilder().build()
    }

    override fun BuildActors(request: BuildActorsRequest): BuildActorsResponse {
      return BuildActorsResponse.newBuilder().build()
    }

    override fun dropFragment(request: DropActorsRequest): DropActorsResponse {
      return DropActorsResponse.newBuilder().build()
    }

    override suspend fun createTaskKt(request: CreateTaskRequest): CreateTaskResponse {
      return CreateTaskResponse.newBuilder().build()
    }
  }

  override fun getOrCreate(node: WorkerNode): TestClient {
    return TestClient()
  }
}
