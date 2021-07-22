package com.risingwave.planner.rel.physical.streaming;

import org.apache.calcite.rel.RelNode;

public class StreamingPlan {
  private final RelNode physicalPlan;

  public StreamingPlan(RelNode physicalPlan) {
    this.physicalPlan = physicalPlan;
  }

  public RelNode getPhysicalPlan() {
    return physicalPlan;
  }
}
