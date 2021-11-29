package com.risingwave.scheduler.stage;

import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import com.risingwave.common.datatype.RisingWaveDataType;
import com.risingwave.node.WorkerNode;
import com.risingwave.planner.rel.physical.RisingWaveBatchPhyRel;
import com.risingwave.planner.rel.physical.RwBatchExchange;
import com.risingwave.proto.computenode.ExchangeNode;
import com.risingwave.proto.computenode.ExchangeSource;
import com.risingwave.proto.computenode.HostAddress;
import com.risingwave.proto.computenode.MergeSortExchangeNode;
import com.risingwave.proto.computenode.TaskSinkId;
import com.risingwave.proto.plan.PlanFragment;
import com.risingwave.proto.plan.PlanNode;
import com.risingwave.scheduler.exchange.Distribution;
import com.risingwave.scheduler.query.Query;
import java.util.NoSuchElementException;
import java.util.Optional;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A QueryStage is part of a distributed physical plan running in one local node. Operators in one
 * stage are pipelined.
 */
public class QueryStage {
  private static final Logger LOGGER = LoggerFactory.getLogger(QueryStage.class);

  private final StageId stageId;
  private final RisingWaveBatchPhyRel root;
  private final Distribution distribution;

  // Augmented fields.
  private final ImmutableMap<StageId, ScheduledStage> exchangeSources;
  private final Query query;
  private final int parallelism;
  private final ImmutableList<WorkerNode> workers;
  // We remark that we must know the parallelism of the next stage
  // so that we can determine the `outputCount` in Hash/Broadcast exchange.
  private final int nextStageParallelism;

  /**
   * Constructor
   *
   * @param stageId stage identifier
   * @param root root of plan tree
   * @param distribution data distribution of the stage
   */
  public QueryStage(StageId stageId, RisingWaveBatchPhyRel root, Distribution distribution) {
    this.stageId = stageId;
    this.root = root;
    this.distribution = distribution;

    // Not yet augmented.
    this.exchangeSources = null;
    this.query = null;
    this.parallelism = 0;
    this.workers = null;
    this.nextStageParallelism = 0;
  }

  private QueryStage(
      QueryStage other,
      Query query,
      ImmutableMap<StageId, ScheduledStage> exchangeSources,
      ImmutableList<WorkerNode> workers,
      int nextStageParallelism) {
    this.stageId = other.stageId;
    this.root = other.root;
    this.distribution = other.distribution;
    this.exchangeSources = exchangeSources;
    this.query = query;
    this.parallelism = workers.size();
    this.workers = workers;
    this.nextStageParallelism = nextStageParallelism;
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
      ImmutableList<WorkerNode> workers,
      int nextStageParallelism) {
    return new QueryStage(this, query, exchangeSources, workers, nextStageParallelism);
  }

  public StageId getStageId() {
    return stageId;
  }

  public int getParallelism() {
    assert parallelism != 0;
    return parallelism;
  }

  public Distribution getDistribution() {
    return this.distribution;
  }

  public ImmutableList<WorkerNode> getWorkers() {
    requireNonNull(workers, "QueryStage must be augmented.");
    return workers;
  }

  public RisingWaveBatchPhyRel getRoot() {
    return root;
  }

  public PlanFragment toPlanFragmentProto(final int taskId) {
    // Rewrite the exchange nodes.
    PlanNode protoRoot = rewriteIfExchange(root, taskId);
    return PlanFragment.newBuilder()
        .setRoot(protoRoot)
        .setExchangeInfo(distribution.toExchangeInfo(this.nextStageParallelism))
        .build();
  }

  private PlanNode rewriteIfExchange(RisingWaveBatchPhyRel relNode, final int taskId) {
    requireNonNull(exchangeSources, "QueryStage must be augmented before serialization.");
    requireNonNull(query, "QueryStage must be augmented before serialization.");

    PlanNode node = relNode.serialize();
    PlanNode.Builder builder = node.toBuilder();
    builder.clearChildren();
    PlanNode.PlanNodeType planNodeType = node.getNodeType();
    if (planNodeType.equals(PlanNode.PlanNodeType.EXCHANGE)
        || planNodeType.equals(PlanNode.PlanNodeType.MERGE_SORT_EXCHANGE)) {
      assert node.getChildrenCount() == 0;
      StageId stageId = query.getExchangeSource((RwBatchExchange) relNode);
      ScheduledStage stage =
          Optional.ofNullable(exchangeSources.get(stageId))
              .orElseThrow(
                  () ->
                      new NoSuchElementException(
                          String.format("stage %s has not been scheduled", stageId.toString())));
      ExchangeNode exchangeNode = createExchange(stage, relNode, taskId);
      if (planNodeType.equals(PlanNode.PlanNodeType.MERGE_SORT_EXCHANGE)) {
        // This try catch is a temporary fix as Any will be gradually removed from the proto.
        try {
          MergeSortExchangeNode mergeSortExchangeNode =
              node.getBody().unpack(MergeSortExchangeNode.class);
          var mergeSortBuilder = mergeSortExchangeNode.toBuilder();
          mergeSortBuilder.setExchangeNode(exchangeNode);
          builder.setBody(Any.pack(mergeSortBuilder.build()));
        } catch (InvalidProtocolBufferException e) {
          var errorMessage =
              "The plan node type is MERGE_SORT_EXCHANGE and expects a MergeSortExchangeNode in Any.";
          LOGGER.error(errorMessage);
          throw new IllegalArgumentException(errorMessage);
        }
      } else {
        builder.setBody(Any.pack(exchangeNode));
      }
    } else {
      for (var child : relNode.getInputs()) {
        builder.addChildren(rewriteIfExchange((RisingWaveBatchPhyRel) child, taskId));
      }
    }
    return builder.build();
  }

  private ExchangeNode createExchange(
      ScheduledStage stage, RisingWaveBatchPhyRel relNode, final int currentTaskId) {
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
              // The exchange sources of the current task must fetch the same sink of all the
              // upstream tasks of
              // all the stages.
              // For example, if the current task is a hash join of parallelism 2. It must have two
              // upstream/child stages.
              // The parallelism of each upstream/child stages can be any number, but their
              // outputCount in
              // exchange info must be 2.
              // The current task is trying to fetch data from both of the upstream/child stages.
              // The sink id
              // must be the same,
              // otherwise the current task cannot fetch data with the same keys.
              // For example:
              // The first task (task id 0) in the current stage should fetch sink id 0 of all the
              // tasks in the previous stage
              // The second task (task id 1) ... sink id 1 ...
              var sinkId =
                  TaskSinkId.newBuilder()
                      .setTaskId(taskId.toTaskIdProto())
                      .setSinkId(currentTaskId);
              var source = ExchangeSource.newBuilder().setSinkId(sinkId).setHost(host).build();
              builder.addSources(source);
            });
    var fieldList = relNode.getInput(0).getRowType().getFieldList();
    for (RelDataTypeField field : fieldList) {
      builder.addInputSchema(convert(field));
    }
    return builder.build();
  }

  private static ExchangeNode.Field convert(RelDataTypeField field) {
    return ExchangeNode.Field.newBuilder()
        .setDataType(((RisingWaveDataType) field.getType()).getProtobufType())
        .build();
  }
}
