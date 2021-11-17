package com.risingwave.planner.rel.physical.batch;

import static com.risingwave.planner.rel.logical.RisingWaveLogicalRel.LOGICAL;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.Any;
import com.risingwave.common.datatype.RisingWaveDataType;
import com.risingwave.planner.rel.common.dist.RwDistributionTraitDef;
import com.risingwave.planner.rel.common.dist.RwDistributions;
import com.risingwave.planner.rel.logical.RwLogicalSort;
import com.risingwave.proto.data.DataType;
import com.risingwave.proto.expr.InputRefExpr;
import com.risingwave.proto.plan.ColumnOrder;
import com.risingwave.proto.plan.OrderByNode;
import com.risingwave.proto.plan.OrderType;
import com.risingwave.proto.plan.PlanNode;
import com.risingwave.proto.plan.TopNNode;
import java.util.ArrayList;
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
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Pair;
import org.apache.commons.lang3.SerializationException;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Sort in Batch convention */
// TODO: convert to distributed plan when offset != NULL
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
    if (offset != null) {
      throw new UnsupportedOperationException("sort with offset is not support");
    }
  }

  public static RwBatchSort create(RelNode input, RelCollation collection) {
    RelOptCluster cluster = input.getCluster();
    RelTraitSet traitSet = input.getTraitSet().plus(BATCH_PHYSICAL).plus(collection);
    return new RwBatchSort(cluster, traitSet, input, collection);
  }

  @Override
  public PlanNode serialize() {
    List<ColumnOrder> columnOrders = new ArrayList<ColumnOrder>();
    List<RelFieldCollation> rfc = collation.getFieldCollations();
    for (RelFieldCollation relFieldCollation : rfc) {
      RexInputRef inputRef =
          getCluster().getRexBuilder().makeInputRef(input, relFieldCollation.getFieldIndex());
      DataType returnType = ((RisingWaveDataType) inputRef.getType()).getProtobufType();
      InputRefExpr inputRefExpr =
          InputRefExpr.newBuilder().setColumnIdx(inputRef.getIndex()).build();
      RelFieldCollation.Direction dir = relFieldCollation.getDirection();
      OrderType orderType;
      if (dir == RelFieldCollation.Direction.ASCENDING) {
        orderType = OrderType.ASCENDING;
      } else if (dir == RelFieldCollation.Direction.DESCENDING) {
        orderType = OrderType.DESCENDING;
      } else {
        throw new SerializationException(String.format("%s direction not supported", dir));
      }
      ColumnOrder columnOrder =
          ColumnOrder.newBuilder()
              .setOrderType(orderType)
              .setInputRef(inputRefExpr)
              .setReturnType(returnType)
              .build();
      columnOrders.add(columnOrder);
    }
    if (fetch != null && offset == null) {
      // serialize to TopNNode
      // FIXME: it's may not optimal to use TopN here, we need to discuss it based on the scale of
      // fetch
      TopNNode.Builder topnNodeBuilder = TopNNode.newBuilder();
      topnNodeBuilder.addAllColumnOrders(columnOrders).setLimit(RexLiteral.intValue(fetch));
      return PlanNode.newBuilder()
          .setNodeType(PlanNode.PlanNodeType.TOP_N)
          .setBody(Any.pack(topnNodeBuilder.build()))
          .addChildren(((RisingWaveBatchPhyRel) input).serialize())
          .build();
    } else {
      // serialize to OrderByNode
      OrderByNode.Builder orderByNodeBuilder = OrderByNode.newBuilder();
      orderByNodeBuilder.addAllColumnOrders(columnOrders);
      return PlanNode.newBuilder()
          .setNodeType(PlanNode.PlanNodeType.ORDER_BY)
          .setBody(Any.pack(orderByNodeBuilder.build()))
          .addChildren(((RisingWaveBatchPhyRel) input).serialize())
          .build();
    }
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

  @Override
  public boolean isEnforcer() {
    if (getTraitSet().contains(BATCH_DISTRIBUTED)) {
      return false;
    } else {
      return super.isEnforcer();
    }
  }

  @Override
  public RelNode convertToDistributed() {
    if (fetch == null) {
      return copy(
          getTraitSet().replace(BATCH_DISTRIBUTED),
          RelOptRule.convert(input, input.getTraitSet().replace(BATCH_DISTRIBUTED)),
          getCollation());
    }
    if (offset == null) {
      return copy(
          getTraitSet().replace(BATCH_DISTRIBUTED).plus(RwDistributions.SINGLETON),
          RelOptRule.convert(
              input,
              input.getTraitSet().replace(BATCH_DISTRIBUTED).plus(RwDistributions.SINGLETON)),
          getCollation());
    }
    throw new UnsupportedOperationException("sort with offset is not support");
  }

  @Override
  public Pair<RelTraitSet, List<RelTraitSet>> deriveTraits(
      final RelTraitSet childTraits, final int childId) {
    if (fetch != null) {
      return null;
    }
    var newTraits = traitSet;
    var dist = childTraits.getTrait(RwDistributionTraitDef.getInstance());
    if (dist != null) {
      newTraits = newTraits.plus(dist);
    }
    return Pair.of(newTraits, ImmutableList.of(childTraits));
  }

  /** Rule for converting sort in logical convention to batch convention */
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

      var newInput = RelOptRule.convert(logicalSort.getInput(), requiredInputTraits);

      return new RwBatchSort(
          rel.getCluster(),
          newTraits,
          newInput,
          logicalSort.getCollation(),
          logicalSort.offset,
          logicalSort.fetch);
    }
  }
}
