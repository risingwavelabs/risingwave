package com.risingwave.planner.rel.logical;

import org.apache.calcite.rel.RelNode;

public class LogicalPlan {
  private final RelNode relNode;

  public LogicalPlan(RelNode relNode) {
    this.relNode = relNode;
  }

  public RelNode getRelNode() {
    return relNode;
  }
}
