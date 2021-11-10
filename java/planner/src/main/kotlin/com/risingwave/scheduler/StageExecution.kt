package com.risingwave.scheduler

import com.google.common.collect.ImmutableList
import com.google.common.collect.ImmutableMap
import com.risingwave.common.exception.PgErrorCode
import com.risingwave.common.exception.PgException
import com.risingwave.node.WorkerNode
import com.risingwave.scheduler.stage.QueryStage
import com.risingwave.scheduler.stage.ScheduledStage
import com.risingwave.scheduler.task.QueryTask
import com.risingwave.scheduler.task.TaskId
import com.risingwave.scheduler.task.TaskManager
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import org.slf4j.LoggerFactory
import java.util.concurrent.ConcurrentHashMap
import java.util.stream.IntStream

class StageExecution(
  private val taskManager: TaskManager,
  private val stage: QueryStage
) {
  companion object {
    private val log = LoggerFactory.getLogger(StageExecution::class.java)
  }

  // Returned value of this execution.
  lateinit var scheduledStage: ScheduledStage
    private set // Read-only by outer users.

  suspend fun execute() {
    // We remark that the task id is a consecutive number from 0 to stage.parallelism - 1.
    val tasks = IntStream.range(0, stage.parallelism)
      .mapToObj { idx: Int -> QueryTask(TaskId(stage.stageId, idx), stage) }
      .collect(ImmutableList.toImmutableList())
    doExecute(tasks)
  }

  internal suspend fun doExecute(tasks: List<QueryTask>) = coroutineScope {
    val scheduleTasks: ConcurrentHashMap<TaskId, WorkerNode> = ConcurrentHashMap(stage.parallelism)
    tasks.map { task ->
      launch {
        try {
          val node = taskManager.schedule(task)
          scheduleTasks[task.taskId] = node
        } catch (e: Exception) {
          log.info("doExecute error:$e")
          throw PgException(PgErrorCode.INTERNAL_ERROR, "task ${task.taskId} failed to schedule")
        }
      }
    }.joinAll() // Wait until all tasks complete.
    log.info("All tasks in stage {} scheduled.", stage.stageId)

    scheduledStage = ScheduledStage(stage, ImmutableMap.copyOf(scheduleTasks))
  }
}
