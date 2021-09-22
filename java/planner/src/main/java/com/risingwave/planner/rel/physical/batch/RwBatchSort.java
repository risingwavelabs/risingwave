package com.risingwave.planner.rel.physical.batch;

import static com.risingwave.execution.context.ExecutionContext.contextOf;
import static com.risingwave.planner.planner.PlannerUtils.isSingleMode;
import static com.risingwave.planner.rel.logical.RisingWaveLogicalRel.LOGICAL;

import com.google.protobuf.Any;
import com.risingwave.planner.rel.common.dist.RwDistributions;
import com.risingwave.planner.rel.logical.RwLogicalSort;
import com.risingwave.planner.rel.serialization.RexToProtoSerializer;
import com.risingwave.proto.expr.ExprNode;
import com.risingwave.proto.plan.OrderByNode;
import com.risingwave.proto.plan.PlanNode;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.commons.lang3.SerializationException;
import org.checkerframework.checker.nullness.qual.Nullable;

public class RwBatchSort extends Sort implements RisingWaveBatchPhyRel {
  public RwBatchSort(
      RelOptCluster cluster, RelTraitSet traits, RelNode child, RelCollation collation) {
    this(cluster, traits, child, collation, null, null);
  }

  public RwBatchSort(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelNode child,
      RelCollation collation,
      @Nullable RexNode offset,
      @Nullable RexNode fetch) {
    super(cluster, traits, child, collation, offset, fetch);
  }

  @Override
  public PlanNode serialize() {
    OrderByNode.Builder orderByNodeBuilder = OrderByNode.newBuilder();
    List<RelFieldCollation> rfc = collation.getFieldCollations();
    for (RelFieldCollation relFieldCollation : rfc) {
      RexInputRef inputRef =
          getCluster().getRexBuilder().makeInputRef(input, relFieldCollation.getFieldIndex());
      ExprNode order = new RexToProtoSerializer().visitInputRef(inputRef);
      RelFieldCollation.Direction dir = relFieldCollation.getDirection();
      OrderByNode.OrderType orderType;
      if (dir == RelFieldCollation.Direction.ASCENDING) {
        orderType = OrderByNode.OrderType.ASCENDING;
      } else if (dir == RelFieldCollation.Direction.DESCENDING) {
        orderType = OrderByNode.OrderType.DESCENDING;
      } else {
        throw new SerializationException(String.format("%s direction not supported", dir));
      }
      orderByNodeBuilder.addOrders(order).addOrderTypes(orderType);
    }
    return PlanNode.newBuilder()
        .setNodeType(PlanNode.PlanNodeType.ORDER_BY)
        .setBody(Any.pack(orderByNodeBuilder.build()))
        .addChildren(((RisingWaveBatchPhyRel) input).serialize())
        .build();
  }

  @Override
  public Sort copy(
      RelTraitSet traitSet,
      RelNode newInput,
      RelCollation newCollation,
      @Nullable RexNode offset,
      @Nullable RexNode fetch) {
    return new RwBatchSort(getCluster(), traitSet, newInput, newCollation, offset, fetch);
  }

  public static class RwBatchSortConverterRule extends ConverterRule {
    public static final RwBatchSortConverterRule INSTANCE =
        Config.INSTANCE
            .withInTrait(LOGICAL)
            .withOutTrait(BATCH_PHYSICAL)
            .withRuleFactory(RwBatchSortConverterRule::new)
            .withOperandSupplier(t -> t.operand(RwLogicalSort.class).anyInputs())
            .withDescription("Converting logical sort to batch sort.")
            .as(Config.class)
            .toRule(RwBatchSortConverterRule.class);

    protected RwBatchSortConverterRule(Config config) {
      super(config);
    }

    @Override
    public @Nullable RelNode convert(RelNode rel) {
      var logicalSort = (RwLogicalSort) rel;

      var requiredInputTraits = logicalSort.getInput().getTraitSet().plus(BATCH_PHYSICAL);

      var newTraits =
          logicalSort.getTraitSet().plus(BATCH_PHYSICAL).plus(logicalSort.getCollation());

      if (isSingleMode(contextOf(rel.getCluster()))) {
        var newInput = RelOptRule.convert(logicalSort.getInput(), requiredInputTraits);

        return new RwBatchSort(
            rel.getCluster(),
            newTraits,
            newInput,
            logicalSort.getCollation(),
            logicalSort.offset,
            logicalSort.fetch);
      } else {
        var distTrait = RwDistributions.hash(getDistributionFields(logicalSort));
        requiredInputTraits = requiredInputTraits.plus(distTrait);

        var newInput = RelOptRule.convert(logicalSort.getInput(), requiredInputTraits);

        newTraits = newTraits.plus(distTrait);

        var batchSort =
            new RwBatchSort(
                rel.getCluster(),
                newTraits,
                newInput,
                logicalSort.getCollation(),
                logicalSort.offset,
                logicalSort.fetch);

        return RwBatchExchange.create(batchSort, RwDistributions.SINGLETON);
      }
    }
  }

  private static int[] getDistributionFields(Sort sort) {
    return sort.getCollation().getFieldCollations().stream()
        .mapToInt(RelFieldCollation::getFieldIndex)
        .toArray();
  }
}
