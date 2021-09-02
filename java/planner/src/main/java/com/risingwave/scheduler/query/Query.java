package com.risingwave.scheduler.query;

import com.risingwave.scheduler.stage.QueryStage;
import com.risingwave.scheduler.stage.StageId;
import java.util.List;
import java.util.Set;

public class Query {
  private final QueryId queryId;
  private final StageGraph graph;

  public Query(QueryId queryId, StageGraph graph) {
    this.queryId = queryId;
    this.graph = graph;
  }

  public Set<StageId> getParentsChecked(StageId stageId) {
    return graph.getParentsChecked(stageId);
  }

  public Set<StageId> getChildrenChecked(StageId stageId) {
    return graph.getChildrenChecked(stageId);
  }

  public QueryStage getQueryStageChecked(StageId stageId) {
    return graph.getQueryStageChecked(stageId);
  }

  public List<StageId> getLeafStages() {
    return graph.getLeafStages();
  }

  public StageId getRootStageId() {
    return graph.getRootStageId();
  }

  public QueryId getQueryId() {
    return queryId;
  }
}
