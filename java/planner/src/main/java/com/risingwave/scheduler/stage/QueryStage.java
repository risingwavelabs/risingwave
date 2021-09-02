package com.risingwave.scheduler.stage;

import com.google.common.collect.ImmutableList;
import com.risingwave.scheduler.task.QueryTask;
import com.risingwave.scheduler.task.TaskId;
import java.util.stream.IntStream;

public class QueryStage {
  private final StageId stageId;
  private final StagePlanInfo planInfo;
  private final ImmutableList<QueryTask> tasks;

  public QueryStage(StageId stageId, StagePlanInfo planInfo) {
    this.stageId = stageId;
    this.planInfo = planInfo;

    tasks =
        IntStream.range(0, planInfo.getParallelism())
            .mapToObj(idx -> new QueryTask(new TaskId(stageId, idx), planInfo))
            .collect(ImmutableList.toImmutableList());
  }

  public StageId getStageId() {
    return stageId;
  }

  public StagePlanInfo getPlanInfo() {
    return planInfo;
  }

  public ImmutableList<QueryTask> getTasks() {
    return tasks;
  }
}
