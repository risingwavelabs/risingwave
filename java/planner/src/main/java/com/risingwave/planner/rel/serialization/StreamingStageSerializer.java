package com.risingwave.planner.rel.serialization;

import com.risingwave.planner.rel.streaming.RisingWaveStreamingRel;
import com.risingwave.planner.rel.streaming.RwStreamExchange;
import com.risingwave.proto.streaming.plan.StreamNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A <code>StreamingStageSerializer</code> generates the proto of partial nodes in the while stream
 * plan.
 */
public class StreamingStageSerializer {
  private static final Logger LOGGER = LoggerFactory.getLogger(StreamingStageSerializer.class);

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
      if (stageRoot.getInputs().size() == 0) {
        LOGGER.info("serialize root has 0 inputs, do nothing");
      } else if (stageRoot.getInputs().size() == 1) {
        RisingWaveStreamingRel child = (RisingWaveStreamingRel) stageRoot.getInputs().get(0);
        // Stop the serialization when encountering exchange.
        if (!(child instanceof RwStreamExchange)) {
          // Only serialize the child if only one non-exchange child exists.
          StreamNode childSerialization = serialize(child);
          builder.addInput(childSerialization);
        } else {
          // Serialize the exchange node.
          builder.addInput(child.serialize());
        }
      } else if (stageRoot.getInputs().size() == 2) {
        RisingWaveStreamingRel leftChild = (RisingWaveStreamingRel) stageRoot.getInputs().get(0);
        // Stop the serialization when encountering exchange.
        if (!(leftChild instanceof RwStreamExchange)) {
          // Only serialize the child if only one non-exchange child exists.
          StreamNode childSerialization = serialize(leftChild);
          builder.addInput(childSerialization);
        } else {
          // Serialize the exchange node.
          builder.addInput(leftChild.serialize());
        }
        RisingWaveStreamingRel rightChild = (RisingWaveStreamingRel) stageRoot.getInputs().get(1);
        // Stop the serialization when encountering exchange.
        if (!(rightChild instanceof RwStreamExchange)) {
          // Only serialize the child if only one non-exchange child exists.
          StreamNode childSerialization = serialize(rightChild);
          builder.addInput(childSerialization);
        } else {
          // Serialize the exchange node.
          builder.addInput(rightChild.serialize());
        }
      } else {
        throw new AssertionError("Plan node with more than 2 children");
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
