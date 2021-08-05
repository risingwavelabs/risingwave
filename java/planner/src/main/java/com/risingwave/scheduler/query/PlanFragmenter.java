package com.risingwave.scheduler.query;

import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.risingwave.planner.rel.physical.batch.BatchPlan;
import com.risingwave.scheduler.shuffle.SinglePartitionSchema;
import com.risingwave.scheduler.stage.QueryStage;
import com.risingwave.scheduler.stage.StageId;
import com.risingwave.scheduler.stage.StagePlanInfo;
import java.util.HashMap;
import java.util.Map;

public class PlanFragmenter {
  private final BatchPlan plan;

  private final QueryId queryId = QueryId.next();
  private final Map<StageId, QueryStage> stages = new HashMap<>();
  private final Map<StageId, StageLinkage> stageLinkages = new HashMap<>();
  private int nextStageId = 0;

  public PlanFragmenter(BatchPlan plan) {
    this.plan = requireNonNull(plan, "plan");
  }

  public Query planDistribution() {
    QueryStage rootStage = createRootStage();

    StageLinkage linkage =
        new StageLinkage(rootStage.getStageId(), ImmutableSet.of(), ImmutableSet.of());
    stageLinkages.put(linkage.getStageId(), linkage);

    return new Query(
        queryId,
        ImmutableMap.copyOf(stages),
        ImmutableMap.copyOf(stageLinkages),
        rootStage.getStageId());
  }

  private QueryStage createRootStage() {
    StagePlanInfo stagePlanInfo = new StagePlanInfo(plan.getRoot(), new SinglePartitionSchema(), 1);
    QueryStage queryStage = new QueryStage(getNextStageId(), stagePlanInfo, ImmutableSet.of());
    stages.put(queryStage.getStageId(), queryStage);
    return queryStage;
  }

  private StageId getNextStageId() {
    return new StageId(queryId, nextStageId++);
  }
}
