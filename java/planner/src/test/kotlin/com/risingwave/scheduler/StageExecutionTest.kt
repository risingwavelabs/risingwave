package com.risingwave.scheduler

import com.google.common.collect.ImmutableList
import com.risingwave.scheduler.query.QueryId
import com.risingwave.scheduler.stage.QueryStage
import com.risingwave.scheduler.stage.StageId
import com.risingwave.scheduler.task.QueryTask
import com.risingwave.scheduler.task.TaskId
import com.risingwave.scheduler.task.TaskManager
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import org.mockito.Mockito
import java.util.stream.IntStream

class StageExecutionTest {
  @Test
  fun testErrorHandling() = runBlocking {
    val taskManager = Mockito.mock(TaskManager::class.java)

    val stage = Mockito.mock(QueryStage::class.java)
    Mockito.`when`(stage.stageId).thenReturn(StageId(QueryId("abc"), 1))

    val tasks = IntStream.range(0, 4)
      .mapToObj { idx: Int -> QueryTask(TaskId(stage.stageId, idx), stage) }
      .collect(ImmutableList.toImmutableList())
    Mockito.`when`(taskManager.schedule(tasks[3])).thenThrow(RuntimeException("task failure"))

    val stageExecution = StageExecution(taskManager, stage)
    try {
      stageExecution.doExecute(tasks)
    } catch (e: Exception) {
    }

    // This test ensures after catching the exception thrown from task 3,
    // there won't be leaked exception.
  }
}
