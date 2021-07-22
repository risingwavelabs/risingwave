package com.risingwave.planner.rel.physical.batch;

import org.apache.calcite.rel.RelNode;

/**
 * Plan for adhoc query execution. The counterpart is streaming plan, which is designed for
 * streaming execution.
 */
public class BatchPlan {
  private final RelNode root;

  public BatchPlan(RelNode root) {
    this.root = root;
  }

  public RelNode getRoot() {
    return root;
  }
}
