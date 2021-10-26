package com.risingwave.scheduler.stage;

import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Any;
import com.risingwave.node.WorkerNode;
import com.risingwave.planner.rel.physical.batch.RisingWaveBatchPhyRel;
import com.risingwave.planner.rel.physical.batch.RwBatchExchange;
import com.risingwave.proto.computenode.ExchangeNode;
import com.risingwave.proto.computenode.ExchangeSource;
import com.risingwave.proto.computenode.HostAddress;
import com.risingwave.proto.computenode.TaskSinkId;
import com.risingwave.proto.plan.PlanFragment;
import com.risingwave.proto.plan.PlanNode;
import com.risingwave.scheduler.exchange.DistributionSchema;
import com.risingwave.scheduler.query.Query;
import java.util.NoSuchElementException;
import java.util.Optional;

/**
 * A QueryStage is part of a distributed physical plan running in one local node. Operators in one
 * stage are pipelined.
 */
public class QueryStage {
  private final StageId stageId;
  private final RisingWaveBatchPhyRel root;
  private final DistributionSchema distributionSchema;

  // Augmented fields.
  private final ImmutableMap<StageId, ScheduledStage> exchangeSources;
  private final Query query;
  private final int parallelism;
  private final ImmutableList<WorkerNode> workers;

  /**
   * Constructor
   *
   * @param stageId stage identifier
   * @param root root of plan tree
   * @param distributionSchema data distribution of the stage
   */
  public QueryStage(
      StageId stageId, RisingWaveBatchPhyRel root, DistributionSchema distributionSchema) {
    this.stageId = stageId;
    this.root = root;
    this.distributionSchema = distributionSchema;

    // Not yet augmented.
    this.exchangeSources = null;
    this.query = null;
    this.parallelism = 0;
    this.workers = null;
  }

  private QueryStage(
      QueryStage other,
      Query query,
      ImmutableMap<StageId, ScheduledStage> exchangeSources,
      ImmutableList<WorkerNode> workers) {
    this.stageId = other.stageId;
    this.root = other.root;
    this.distributionSchema = other.distributionSchema;
    this.exchangeSources = exchangeSources;
    this.query = query;
    this.parallelism = workers.size();
    this.workers = workers;
  }

  /**
   * Augments the stage with more information.
   *
   * @param exchangeSources the children of this stage.
   * @param workers the compute-nodes assigned to execute this stage, one node per-task.
   */
  public QueryStage augmentInfo(
      Query query,
      ImmutableMap<StageId, ScheduledStage> exchangeSources,
      ImmutableList<WorkerNode> workers) {
    return new QueryStage(this, query, exchangeSources, workers);
  }

  public StageId getStageId() {
    return stageId;
  }

  public int getParallelism() {
    assert parallelism != 0;
    return parallelism;
  }

  public ImmutableList<WorkerNode> getWorkers() {
    requireNonNull(workers, "QueryStage must be augmented.");
    return workers;
  }

  public RisingWaveBatchPhyRel getRoot() {
    return root;
  }

  public PlanFragment toPlanFragmentProto() {
    // Rewrite the exchange nodes.
    PlanNode protoRoot = rewriteIfExchange(root);
    return PlanFragment.newBuilder()
        .setRoot(protoRoot)
        .setExchangeInfo(distributionSchema.toExchangeInfo())
        .build();
  }

  private PlanNode rewriteIfExchange(RisingWaveBatchPhyRel relNode) {
    requireNonNull(exchangeSources, "QueryStage must be augmented before serialization.");
    requireNonNull(query, "QueryStage must be augmented before serialization.");

    PlanNode node = relNode.serialize();
    PlanNode.Builder builder = node.toBuilder();
    builder.clearChildren();
    if (node.getNodeType().equals(PlanNode.PlanNodeType.EXCHANGE)) {
      assert node.getChildrenCount() == 0;
      StageId stageId = query.getExchangeSource((RwBatchExchange) relNode);
      ScheduledStage stage =
          Optional.ofNullable(exchangeSources.get(stageId))
              .orElseThrow(
                  () ->
                      new NoSuchElementException(
                          String.format("stage %s has not been scheduled", stageId.toString())));
      builder.setBody(Any.pack(createExchange(stage)));
    } else {
      for (var child : relNode.getInputs()) {
        builder.addChildren(rewriteIfExchange((RisingWaveBatchPhyRel) child));
      }
    }
    return builder.build();
  }

  private static ExchangeNode createExchange(ScheduledStage stage) {
    var builder = ExchangeNode.newBuilder().setSourceStageId(stage.getStageId().toStageIdProto());
    stage
        .getAssignments()
        .forEach(
            (taskId, node) -> {
              var host =
                  HostAddress.newBuilder()
                      .setHost(node.getRpcEndPoint().getHost())
                      .setPort(node.getRpcEndPoint().getPort())
                      .build();
              // TODO: attach sink id to every exchange node.
              var sinkId = TaskSinkId.newBuilder().setTaskId(taskId.toTaskIdProto()).setSinkId(0);
              var source = ExchangeSource.newBuilder().setSinkId(sinkId).setHost(host).build();
              builder.addSources(source);
            });
    return builder.build();
  }
}
