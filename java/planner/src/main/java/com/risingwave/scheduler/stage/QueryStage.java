package com.risingwave.scheduler.stage;

import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Any;
import com.risingwave.planner.rel.physical.batch.RisingWaveBatchPhyRel;
import com.risingwave.planner.rel.physical.batch.RwBatchExchange;
import com.risingwave.proto.computenode.ExchangeNode;
import com.risingwave.proto.computenode.ExchangeSource;
import com.risingwave.proto.computenode.HostAddress;
import com.risingwave.proto.computenode.TaskSinkId;
import com.risingwave.proto.plan.PlanFragment;
import com.risingwave.proto.plan.PlanNode;
import com.risingwave.scheduler.query.Query;
import com.risingwave.scheduler.shuffle.PartitionSchema;
import java.util.NoSuchElementException;
import java.util.Optional;

public class QueryStage {
  private final StageId stageId;
  private final RisingWaveBatchPhyRel root;
  private final PartitionSchema partitionSchema;
  private final int parallelism;

  // Augmented fields.
  private final ImmutableMap<StageId, ScheduledStage> stages;
  private final Query query;

  public QueryStage(
      StageId stageId,
      RisingWaveBatchPhyRel root,
      PartitionSchema partitionSchema,
      int parallelism) {
    this.stageId = stageId;
    this.root = root;
    this.partitionSchema = partitionSchema;
    this.parallelism = parallelism;
    this.stages = null; // Not yet augmented.
    this.query = null;
  }

  private QueryStage(QueryStage other, Query query, ImmutableMap<StageId, ScheduledStage> stages) {
    this.stageId = other.stageId;
    this.root = other.root;
    this.partitionSchema = other.partitionSchema;
    this.parallelism = other.parallelism;
    this.stages = stages;
    this.query = query;
  }

  public QueryStage augmentScheduledInfo(
      Query query, ImmutableMap<StageId, ScheduledStage> stages) {
    return new QueryStage(this, query, stages);
  }

  public StageId getStageId() {
    return stageId;
  }

  public int getParallelism() {
    return parallelism;
  }

  public RisingWaveBatchPhyRel getRoot() {
    return root;
  }

  public PlanFragment toPlanFragmentProto() {
    // Rewrite the exchange nodes.
    PlanNode protoRoot = rewriteIfExchange(root);
    return PlanFragment.newBuilder()
        .setRoot(protoRoot)
        .setShuffleInfo(partitionSchema.toShuffleInfo())
        .build();
  }

  private PlanNode rewriteIfExchange(RisingWaveBatchPhyRel relNode) {
    requireNonNull(stages, "QueryStage must be augmented before serialization.");
    requireNonNull(query, "QueryStage must be augmented before serialization.");

    PlanNode node = relNode.serialize();
    PlanNode.Builder builder = node.toBuilder();
    builder.clearChildren();
    if (node.getNodeType().equals(PlanNode.PlanNodeType.EXCHANGE)) {
      assert node.getChildrenCount() == 0;
      StageId stageId = query.getExchangeSource((RwBatchExchange) relNode);
      ScheduledStage stage =
          Optional.ofNullable(stages.get(stageId))
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
