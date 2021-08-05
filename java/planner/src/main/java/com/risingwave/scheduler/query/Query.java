package com.risingwave.scheduler.query;

import com.google.common.collect.ImmutableMap;
import com.risingwave.common.exception.PgErrorCode;
import com.risingwave.common.exception.PgException;
import com.risingwave.scheduler.stage.QueryStage;
import com.risingwave.scheduler.stage.StageId;
import java.util.Optional;
import java.util.stream.Stream;

public class Query {
  private final QueryId queryId;
  private final ImmutableMap<StageId, QueryStage> stages;
  private final ImmutableMap<StageId, StageLinkage> stageLinkages;
  private final StageId rootStageId;

  public Query(
      QueryId queryId,
      ImmutableMap<StageId, QueryStage> stages,
      ImmutableMap<StageId, StageLinkage> stageLinkages,
      StageId rootStageId) {
    this.queryId = queryId;
    this.stages = stages;
    this.stageLinkages = stageLinkages;
    this.rootStageId = rootStageId;
  }

  public Optional<QueryStage> getQueryStage(StageId stageId) {
    return Optional.ofNullable(stages.get(stageId));
  }

  public QueryStage getQueryStageChecked(StageId stageId) {
    return getQueryStage(stageId)
        .orElseThrow(
            () -> new PgException(PgErrorCode.INTERNAL_ERROR, "Unknown stage id: %s", stageId));
  }

  public Optional<StageLinkage> getStageLinkage(StageId stageId) {
    return Optional.ofNullable(stageLinkages.get(stageId));
  }

  public StageLinkage getStageLinkageChecked(StageId stageId) {
    return getStageLinkage(stageId)
        .orElseThrow(
            () -> new PgException(PgErrorCode.INTERNAL_ERROR, "Unknown stage id: %s", stageId));
  }

  /**
   * Find stages without dependencies.
   *
   * @return Stages without dependencies.
   */
  public Stream<StageId> getLeafStages() {
    return stageLinkages.keySet().stream()
        .filter(stageId -> stageLinkages.get(stageId).getChildren().isEmpty());
  }

  public QueryId getQueryId() {
    return queryId;
  }

  public StageId getRootStageId() {
    return rootStageId;
  }
}
