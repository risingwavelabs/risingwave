package com.risingwave.planner.rel.serialization;

import com.risingwave.planner.rel.streaming.RisingWaveStreamingRel;
import com.risingwave.proto.streaming.plan.StreamNode;

/**
 * A <code>StreamingPlanSerializer</code> generates the proto of raw streaming plan. It will be used
 * by the fragmenter in the Meta service.
 */
public class StreamingPlanSerializer {
  public static StreamNode serialize(RisingWaveStreamingRel root) {
    StreamNode node = root.serialize();
    StreamNode.Builder builder = node.toBuilder();
    root.getInputs().forEach(input -> builder.addInput(serialize((RisingWaveStreamingRel) input)));
    return builder.build();
  }
}
