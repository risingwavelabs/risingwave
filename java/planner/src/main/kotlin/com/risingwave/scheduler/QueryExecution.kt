package com.risingwave.scheduler

import com.google.common.base.Verify
import com.google.common.collect.ImmutableMap
import com.google.common.collect.Maps
import com.risingwave.scheduler.query.Query
import com.risingwave.scheduler.stage.ScheduledStage
import com.risingwave.scheduler.stage.StageId
import com.risingwave.scheduler.task.TaskManager
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.Channel.Factory.UNLIMITED
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory
import java.util.Objects
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentMap

/**
 * Schedules a query. <br>
 * The stages scheduling goes from bottom (leaf stages) to top (root stage). <br>
 * To start a stage, all its dependencies must have been scheduled, because their physical
 * properties, like node assignment information, are required. For example, Exchange requires the
 * children's ExchangeSource, which couldn't be known unless scheduled.
 */
class QueryExecution(
  private val query: Query,
  private val taskManager: TaskManager,
  private val resourceManager: ResourceManager,
  private val resultFuture: CompletableFuture<QueryResultLocation>,
) {
  companion object {
    private val log = LoggerFactory.getLogger(QueryExecution::class.java)
  }

  private val stageExecutions: ConcurrentMap<StageId, StageExecution> = ConcurrentHashMap()
  private var scheduledStages: Map<StageId, ScheduledStage> = Maps.newHashMap()

  /** Start an asynchronous execution. */
  fun start() {
    runBlocking {
      launch {
        execute()
      }
    }
  }

  /** Executes and waits all the scheduling works. */
  private suspend fun execute() = coroutineScope {
    // The sender of this channel produces stages that have been scheduled.
    // The receiver consumes the scheduled stage, deciding whether to fire
    // other stages to the channel, until finally all stages are scheduled.
    // TODO: close the channel upon termination.
    val channel = Channel<ScheduledStage>(UNLIMITED)

    for (stageId in query.leafStages) {
      scheduleStage(stageId, channel)
    }

    while (true) {
      val stage = channel.receive()
      scheduledStages = scheduledStages + Pair(stage.stageId, stage)

      // Start the stages that have all the dependencies scheduled.
      val parents = query.getParentsChecked(stage.stageId)
      for (parentId in parents) {
        if (allDependenciesScheduled(parentId)) {
          scheduleStage(parentId, channel)
        }
      }

      // Now the entire DAG is scheduled.
      if (stage.stageId == query.rootStageId) {
        log.info("Finished query ${query.queryId} scheduling")
        Verify.verify(stage.assignments.size == 1, "Root stage assignment size must be 1")
        val (taskId, node) = stage.assignments.entries.iterator().next()
        val resultLocation = QueryResultLocation(taskId, node)
        resultFuture.complete(resultLocation)
        break
      }
    }
  }

  private suspend fun scheduleStage(stageId: StageId, channel: Channel<ScheduledStage>) = coroutineScope {
    val stage = createOrGetStageExecution(stageId)
    launch {
      stage.execute()
      channel.send(stage.scheduledStage)
    }
  }

  private fun createOrGetStageExecution(stageId: StageId): StageExecution {
    val augmentedStage = resourceManager.schedule(query, stageId, getStageChildren(stageId))
    return stageExecutions.computeIfAbsent(stageId) {
      StageExecution(taskManager, augmentedStage)
    }
  }

  private fun getStageChildren(stageId: StageId): ImmutableMap<StageId, ScheduledStage> {
    val childIds = query.getChildrenChecked(stageId)
    val builder = ImmutableMap.Builder<StageId, ScheduledStage>()
    childIds.forEach { id: StageId ->
      val stage = scheduledStages[id]
      Objects.requireNonNull(stage, "child must have been scheduled!")
      builder.put(stage!!.stageId, stage)
    }
    return builder.build()
  }

  // Whether all children of the stage has been scheduled.
  private fun allDependenciesScheduled(stageId: StageId): Boolean {
    return scheduledStages.keys.containsAll(query.getChildrenChecked(stageId))
  }
}
