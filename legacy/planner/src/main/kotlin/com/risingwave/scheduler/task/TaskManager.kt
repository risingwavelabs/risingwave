package com.risingwave.scheduler.task

import com.risingwave.node.WorkerNode

interface TaskManager {
  suspend fun schedule(task: QueryTask): WorkerNode
}
