package com.risingwave.rpc

import com.risingwave.node.WorkerNode
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

    override fun updateFragment(request: UpdateFragmentRequest): UpdateFragmentResponse {
      return UpdateFragmentResponse.newBuilder().build()
    }

    override fun buildFragment(request: BuildFragmentRequest): BuildFragmentResponse {
      return BuildFragmentResponse.newBuilder().build()
    }

    override fun dropFragment(request: DropFragmentsRequest): DropFragmentsResponse {
      return DropFragmentsResponse.newBuilder().build()
    }

    override suspend fun createTaskKt(request: CreateTaskRequest): CreateTaskResponse {
      return CreateTaskResponse.newBuilder().build()
    }
  }

  override fun getOrCreate(node: WorkerNode): TestClient {
    return TestClient()
  }
}
