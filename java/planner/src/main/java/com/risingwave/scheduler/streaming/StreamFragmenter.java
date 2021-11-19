package com.risingwave.scheduler.streaming;

import static java.util.Objects.requireNonNull;

import com.risingwave.execution.context.ExecutionContext;
import com.risingwave.planner.rel.physical.streaming.RisingWaveStreamingRel;
import com.risingwave.planner.rel.physical.streaming.RwStreamExchange;
import com.risingwave.planner.rel.physical.streaming.StreamingPlan;
import com.risingwave.scheduler.streaming.graph.StreamFragment;
import com.risingwave.scheduler.streaming.graph.StreamGraph;
import com.risingwave.scheduler.streaming.graph.StreamStage;
import com.risingwave.scheduler.streaming.graph.StreamStageGraph;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;

/**
 * A <code>StreamFragmenter</code> generates the proto for interconnected actors for a streaming
 * pipeline.
 */
public class StreamFragmenter {
  private int nextStageId = 0;

  private final StreamStageGraph.StageBuilder stageBuilder = StreamStageGraph.newBuilder();

  /** The <code>StreamFragmenter</code> initializer. */
  public StreamFragmenter() {}

  /**
   * Build a stream graph in two steps. (1) Break the streaming plan into stages with their
   * dependency. (2) Duplicate each stage as parallel fragments.
   *
   * @param streamingPlan The generated streaming plan from Calcite optimizer.
   * @param context The `ExecutionContext` used for in the leader node.
   * @return A graph representation of the streaming job.
   */
  public static StreamGraph generateGraph(StreamingPlan streamingPlan, ExecutionContext context) {
    requireNonNull(streamingPlan, "plan");
    StreamFragmenter fragmenter = new StreamFragmenter();
    StreamStageGraph streamStageGraph = fragmenter.generateStageGraph(streamingPlan, context);
    StreamGraph streamGraph = fragmenter.buildGraphFromStageGraph(streamStageGraph, context);
    return streamGraph;
  }

  /**
   * Break the streaming plan into stages according to their dependency.
   *
   * @param streamingPlan he generated streaming plan from Calcite optimizer.
   * @param context The `ExecutionContext` used in the leader node.
   * @return Annotate each streaming plan node with a stage id.
   */
  public StreamStageGraph generateStageGraph(
      StreamingPlan streamingPlan, ExecutionContext context) {
    var rootStage = newStreamStage(streamingPlan.getStreamingPlan());
    stageBuilder.addStage(rootStage);
    buildStage(rootStage, streamingPlan.getStreamingPlan());
    return stageBuilder.build(rootStage.getStageId());
  }

  /**
   * Recursively build the stage by attaching each node to a stage.
   *
   * @param curStage The current stage to be speculated by the procedure.
   * @param node The current streaming `RelNode` to be speculated by the procedure.
   */
  private void buildStage(StreamStage curStage, RisingWaveStreamingRel node) {
    for (RelNode rn : node.getInputs()) {
      if (rn instanceof RwStreamExchange) {
        RisingWaveStreamingRel child = (RisingWaveStreamingRel) rn;
        StreamStage childStage = newStreamStage(child);
        stageBuilder.addStage(childStage);
        stageBuilder.linkToChild(curStage.getStageId(), childStage.getStageId());
        buildStage(childStage, child);
      } else {
        buildStage(curStage, (RisingWaveStreamingRel) rn);
      }
    }
  }

  private StreamStage newStreamStage(RisingWaveStreamingRel node) {
    var stage = new StreamStage(nextStageId++, node);
    return stage;
  }

  /**
   * Build a stream graph from a stage graph.
   *
   * @param streamStageGraph The stage graph of a streaming plan.
   * @param context The `ExecutionContext` used in the leader node.
   * @return A stream graph encoding dependencies among stream fragments.
   */
  public StreamGraph buildGraphFromStageGraph(
      StreamStageGraph streamStageGraph, ExecutionContext context) {
    StreamGraph.GraphBuilder graphBuilder = StreamGraph.newBuilder();
    // Start from the root stage.
    StreamStage rootStage = streamStageGraph.getRootStage();
    buildFragmentsInStage(
        rootStage, streamStageGraph, Collections.emptyList(), graphBuilder, context);
    return graphBuilder.build();
  }

  private void buildFragmentsInStage(
      StreamStage currentStage,
      StreamStageGraph streamStageGraph,
      List<Integer> lastStageFragmentList,
      StreamGraph.GraphBuilder graphBuilder,
      ExecutionContext context) {

    List<Integer> currentStageFragmentList = new ArrayList();
    int rootStageId = streamStageGraph.getRootStage().getStageId();
    if (currentStage.getStageId() == rootStageId) {
      // Root stage, generate an actor without dispatcher.
      StreamFragment rootFragment =
          newStreamFragment(currentStage.getRoot(), context.getStreamManager());
      // Add blackhole dispatcher.
      StreamFragment.FragmentBuilder builder = rootFragment.toBuilder();
      builder.addBlackHoleDispatcher();
      // Add fragment. No dependency is needed.
      graphBuilder.addFragment(rootFragment.getId(), builder.build());
      currentStageFragmentList.add(rootFragment.getId());

    } else if (streamStageGraph.hasDownstream(currentStage.getStageId())) {
      // Stages in the middle.
      // Generate one or multiple actors and connect them to the next stage.
      // Currently, we assume the parallel degree is at least 4, and grows linearly with more worker
      // nodes added.
      int parallelDegree = Integer.max(context.getWorkerNodeManager().allNodes().size() * 2, 4);
      for (int i = 0; i < parallelDegree; i++) {
        if (currentStage.getRoot() instanceof RwStreamExchange) {
          RwStreamExchange exchange = (RwStreamExchange) currentStage.getRoot();
          StreamFragment fragment = newStreamFragment(exchange, context.getStreamManager());
          int fragmentId = fragment.getId();
          StreamFragment.FragmentBuilder builder = fragment.toBuilder();

          // Add dispatcher.
          RelDistribution distribution = exchange.getDistribution();
          if (distribution.getType() == RelDistribution.Type.BROADCAST_DISTRIBUTED) {
            builder.addBroadcastDispatcher();
          } else if (distribution.getType() == RelDistribution.Type.HASH_DISTRIBUTED) {
            builder.addHashDispatcher(distribution.getKeys());
          } else if (distribution.getType() == RelDistribution.Type.SINGLETON) {
            builder.addSimpleDispatcher();
          }

          // Add fragment and dependencies.
          graphBuilder.addFragment(fragmentId, builder.build());
          for (int lastId : lastStageFragmentList) {
            graphBuilder.addDependency(currentStage.getStageId(), fragmentId, lastId);
          }
          currentStageFragmentList.add(fragmentId);

        } else if (lastStageFragmentList.size() == 1) {
          StreamFragment fragment =
              newStreamFragment(currentStage.getRoot(), context.getStreamManager());
          int fragmentId = fragment.getId();
          graphBuilder.addFragment(fragmentId, fragment);
          int lastStageFragmentId = lastStageFragmentList.get(0);
          // Add dispatcher.
          StreamFragment.FragmentBuilder builder = fragment.toBuilder();
          builder.addSimpleDispatcher();
          // Add fragment and dependencies.
          graphBuilder.addFragment(fragmentId, builder.build());
          graphBuilder.addDependency(currentStage.getStageId(), fragmentId, lastStageFragmentId);
          currentStageFragmentList.add(fragment.getId());
        }
      }
    } else {
      // Stage on the source.
      int parallelDegree = context.getWorkerNodeManager().allNodes().size();
      for (int i = 0; i < parallelDegree; i++) {
        RwStreamExchange exchange = (RwStreamExchange) currentStage.getRoot();
        StreamFragment fragment = newStreamFragment(exchange, context.getStreamManager());
        int fragmentId = fragment.getId();
        StreamFragment.FragmentBuilder builder = fragment.toBuilder();

        // Add dispatcher.
        RelDistribution distribution = exchange.getDistribution();
        if (distribution.getType() == RelDistribution.Type.BROADCAST_DISTRIBUTED) {
          builder.addBroadcastDispatcher();
        } else if (distribution.getType() == RelDistribution.Type.HASH_DISTRIBUTED) {
          builder.addHashDispatcher(distribution.getKeys());
        }
        // Add fragment and dependencies.
        graphBuilder.addFragment(fragmentId, builder.build());
        for (int lastId : lastStageFragmentList) {
          graphBuilder.addDependency(currentStage.getStageId(), fragmentId, lastId);
        }
        currentStageFragmentList.add(fragmentId);
      }
    }

    // Recursively generating fragments on the downstream level.
    if (streamStageGraph.hasDownstream(currentStage.getStageId())) {
      for (int id : streamStageGraph.getDownstreamStages(currentStage.getStageId())) {
        StreamStage downstreamStage = streamStageGraph.getStageById(id);
        buildFragmentsInStage(
            downstreamStage, streamStageGraph, currentStageFragmentList, graphBuilder, context);
      }
    }
  }

  private StreamFragment newStreamFragment(
      RisingWaveStreamingRel root, StreamManager streamManager) {
    int id = streamManager.createFragment();
    StreamFragment.FragmentBuilder builder = StreamFragment.newBuilder(id, root);
    return builder.build();
  }
}
