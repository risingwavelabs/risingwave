package com.risingwave.rpc

import com.risingwave.node.WorkerNode
import com.risingwave.proto.computenode.CreateTaskRequest
import com.risingwave.proto.computenode.CreateTaskResponse
import com.risingwave.proto.computenode.GetDataRequest
import com.risingwave.proto.computenode.GetDataResponse

class TestComputeClientManager : ComputeClientManager {
  class TestClient : ComputeClient {
    override fun createTask(request: CreateTaskRequest): CreateTaskResponse {
      return CreateTaskResponse.newBuilder().build()
    }

    override fun getData(request: GetDataRequest): Iterator<GetDataResponse> {
      val responses = ArrayList<GetDataResponse>()
      return responses.iterator()
    }

    override suspend fun createTaskKt(request: CreateTaskRequest): CreateTaskResponse {
      return CreateTaskResponse.newBuilder().build()
    }
  }

  override fun getOrCreate(node: WorkerNode): TestClient {
    return TestClient()
  }
}
