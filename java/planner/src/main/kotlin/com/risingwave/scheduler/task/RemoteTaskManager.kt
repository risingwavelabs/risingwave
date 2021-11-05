package com.risingwave.scheduler.task

import com.risingwave.node.WorkerNode
import com.risingwave.proto.common.Status
import com.risingwave.proto.computenode.CreateTaskRequest
import com.risingwave.proto.computenode.CreateTaskResponse
import com.risingwave.rpc.ComputeClientManager
import org.slf4j.LoggerFactory
import java.util.Objects
import javax.inject.Inject
import javax.inject.Singleton

@Singleton
class RemoteTaskManager @Inject constructor(
  clientManager: ComputeClientManager
) : TaskManager {

  companion object {
    private val log = LoggerFactory.getLogger(RemoteTaskManager::class.java)
  }

  private val clientManager: ComputeClientManager

  init {
    this.clientManager = Objects.requireNonNull(clientManager, "clientManager")
  }

  override suspend fun schedule(task: QueryTask): WorkerNode {
    val taskId: TaskId = task.taskId
    val node: WorkerNode = task.queryStage.workers[taskId.id]
    val response = sendCreateTaskRequest(task, node)
    if (response.status.code != Status.Code.OK) {
      log.error("Failed to create task: ${response.status.code}, ${response.status.message}")
    } else {
      log.info("Succeeded to create task: ${task.taskId}")
    }
    return node
  }

  private suspend fun sendCreateTaskRequest(task: QueryTask, node: WorkerNode): CreateTaskResponse {
    val client = clientManager.getOrCreate(node)
    val request: CreateTaskRequest = CreateTaskRequest.newBuilder()
      .setTaskId(task.taskId.toTaskIdProto())
      .setPlan(task.queryStage.toPlanFragmentProto())
      .build()
    return client.createTaskKt(request)
  }
}
