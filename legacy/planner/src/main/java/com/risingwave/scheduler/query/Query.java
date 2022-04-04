package com.risingwave.scheduler.query;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.risingwave.planner.rel.physical.RwBatchExchange;
import com.risingwave.scheduler.stage.QueryStage;
import com.risingwave.scheduler.stage.StageId;

/** A query to be executed by the scheduler. */
public class Query {
  private final QueryId queryId;
  private final StageGraph graph;

  public Query(QueryId queryId, StageGraph graph) {
    this.queryId = queryId;
    this.graph = graph;
  }

  public ImmutableSet<StageId> getParentsChecked(StageId stageId) {
    return graph.getParentsChecked(stageId);
  }

  public ImmutableSet<StageId> getChildrenChecked(StageId stageId) {
    return graph.getChildrenChecked(stageId);
  }

  public QueryStage getQueryStageChecked(StageId stageId) {
    return graph.getQueryStageChecked(stageId);
  }

  public ImmutableList<StageId> getLeafStages() {
    return graph.getLeafStages();
  }

  public StageId getRootStageId() {
    return graph.getRootStageId();
  }

  /**
   * @param node the exchange node
   * @return which stage to exchange from
   */
  public StageId getExchangeSource(RwBatchExchange node) {
    return graph.getExchangeSource(node);
  }

  public QueryId getQueryId() {
    return queryId;
  }

  @Override
  public String toString() {
    return "Query id: " + queryId + "\n" + "Stage graph: \n" + graph;
  }
}
