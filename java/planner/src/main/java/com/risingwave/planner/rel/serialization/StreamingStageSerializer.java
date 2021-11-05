package com.risingwave.planner.rel.serialization;

import com.risingwave.common.exception.PgErrorCode;
import com.risingwave.common.exception.PgException;
import com.risingwave.planner.rel.physical.streaming.RisingWaveStreamingRel;
import com.risingwave.planner.rel.physical.streaming.RwStreamExchange;
import com.risingwave.proto.streaming.plan.StreamNode;

/**
 * A <code>StreamingStageSerializer</code> generates the proto of partial nodes in the while stream
 * plan.
 */
public class StreamingStageSerializer {
  /**
   * Serialize the root node to proto recursively until reach the next stage (separated by <code>
   * RwStreamExchange</code>).
   *
   * @param stageRoot The current stage root streaming plan node.
   * @return The <code>StreamNode</code> proto of the current stage root node.
   */
  public static StreamNode serialize(RisingWaveStreamingRel stageRoot) {
    // If root is exchange then serialize its children.
    if (stageRoot instanceof RwStreamExchange) {
      RisingWaveStreamingRel child = (RisingWaveStreamingRel) stageRoot.getInputs().get(0);
      return serialize(child);
    } else {
      StreamNode node = stageRoot.serialize();
      StreamNode.Builder builder = node.toBuilder();
      // Remark 2021.11.05: we have not considered join yet.
      if (stageRoot.getInputs().size() == 1) {
        RisingWaveStreamingRel child = (RisingWaveStreamingRel) stageRoot.getInputs().get(0);
        // Stop the serialization when encountering exchange.
        if (!(child instanceof RwStreamExchange)) {
          // Only serialize the child if only one non-exchange child exists.
          StreamNode childSerialization = serialize(child);
          builder.setInput(childSerialization);
        }
      } else if (stageRoot.getInputs().size() >= 2) {
        throw new PgException(
            PgErrorCode.INTERNAL_ERROR, "Stage serializer for tree plans not implemented yet.");
      }
      return builder.build();
    }
  }

  /**
   * Generate the explanation of the streaming plan in the current stage.
   *
   * @param stageRoot The current stage root streaming plan node.
   * @return The explained plan starting from the current stage.
   */
  public static String explainStage(RisingWaveStreamingRel stageRoot, int level) {
    String explain = stageRoot.explain();
    explain = explain.split("\n")[0];
    if (stageRoot.getInputs().size() == 1) {
      RisingWaveStreamingRel child = (RisingWaveStreamingRel) stageRoot.getInputs().get(0);
      if (!(child instanceof RwStreamExchange)) {
        String explainChild = explainStage(child, level + 1);
        explain = explain + "\n";
        for (int i = 0; i <= level; i++) {
          explain = explain + "  ";
        }
        explain = explain + explainChild;
      }
    }
    return explain;
  }
}
