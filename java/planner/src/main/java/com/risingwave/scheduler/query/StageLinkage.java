package com.risingwave.scheduler.query;

import com.google.common.collect.ImmutableSet;
import com.risingwave.scheduler.stage.StageId;

public class StageLinkage {
  /** Current stage id. */
  private final StageId stageId;
  /** Stages who depend on this stage. */
  private final ImmutableSet<StageId> parents;
  /** Stages this stage depends on. */
  private final ImmutableSet<StageId> children;

  public StageLinkage(
      StageId stageId, ImmutableSet<StageId> parents, ImmutableSet<StageId> children) {
    this.stageId = stageId;
    this.parents = parents;
    this.children = children;
  }

  public ImmutableSet<StageId> getParents() {
    return parents;
  }

  public ImmutableSet<StageId> getChildren() {
    return children;
  }

  public StageId getStageId() {
    return stageId;
  }
}
