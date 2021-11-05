package com.risingwave.scheduler.streaming.graph;

import com.risingwave.planner.rel.physical.streaming.RisingWaveStreamingRel;

/**
 * A stream stage is a part of streaming plan, such that each stage can be parallelized by multiple
 * duplicated actors. Stream executors in each stream stage is pipelined.
 */
public class StreamStage {
  private final int stageId;
  private final RisingWaveStreamingRel root;

  /**
   * Constructor function.
   *
   * @param stageId stage identifier.
   * @param root root of streaming plan tree in this stage.
   */
  public StreamStage(int stageId, RisingWaveStreamingRel root) {
    this.stageId = stageId;
    this.root = root;
  }

  /** Get stage id */
  public int getStageId() {
    return stageId;
  }

  /** Get root streaming `RelNode` */
  public RisingWaveStreamingRel getRoot() {
    return root;
  }
}
