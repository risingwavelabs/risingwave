package com.risingwave.planner.rel.streaming;

import static com.risingwave.planner.rel.logical.RisingWaveLogicalRel.LOGICAL;

import com.risingwave.common.datatype.RisingWaveDataType;
import com.risingwave.planner.metadata.RisingWaveRelMetadataQuery;
import com.risingwave.planner.rel.logical.RwLogicalSort;
import com.risingwave.proto.data.DataType;
import com.risingwave.proto.expr.InputRefExpr;
import com.risingwave.proto.plan.ColumnOrder;
import com.risingwave.proto.plan.OrderType;
import com.risingwave.proto.streaming.plan.StreamNode;
import com.risingwave.proto.streaming.plan.TopNNode;
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
import org.apache.commons.lang3.SerializationException;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Stream Sort */
public class RwStreamSort extends Sort implements RisingWaveStreamingRel {

  public RwStreamSort(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelNode child,
      RelCollation collation,
      @Nullable RexNode offset,
      @Nullable RexNode fetch) {
    super(cluster, traits, child, collation, offset, fetch);
  }

  @Override
  public StreamNode serialize() {
    // Here we serialize RwStreamSort to topN executor in backend, but records will
    // not be sorted here. The sort operation will be achieved through keeping
    // collation in `RwStreamMaterializedView`.
    var primaryKeyIndices =
        ((RisingWaveRelMetadataQuery) getCluster().getMetadataQuery()).getPrimaryKeyIndices(this);

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

    TopNNode.Builder topnBuilder = TopNNode.newBuilder();
    topnBuilder.addAllColumnOrders(columnOrders);
    if (fetch != null) {
      topnBuilder.setLimit(RexLiteral.intValue(fetch));
    }
    if (offset != null) {
      topnBuilder.setOffset(RexLiteral.intValue(offset));
    }

    return StreamNode.newBuilder()
        .setTopNNode(topnBuilder)
        .addAllPkIndices(primaryKeyIndices)
        .build();
  }

  @Override
  public <T> RwStreamingRelVisitor.Result<T> accept(RwStreamingRelVisitor<T> visitor) {
    return visitor.visit(this);
  }

  @Override
  public Sort copy(
      RelTraitSet traitSet,
      RelNode newInput,
      RelCollation newCollation,
      @Nullable RexNode offset,
      @Nullable RexNode fetch) {
    return new RwStreamSort(this.getCluster(), traitSet, newInput, newCollation, offset, fetch);
  }

  /** Rule for converting logical sort to stream sort */
  public static class StreamSortConverterRule extends ConverterRule {
    public static final RwStreamSort.StreamSortConverterRule INSTANCE =
        Config.INSTANCE
            .withInTrait(LOGICAL)
            .withOutTrait(STREAMING)
            .withRuleFactory(RwStreamSort.StreamSortConverterRule::new)
            .withOperandSupplier(t -> t.operand(RwLogicalSort.class).anyInputs())
            .withDescription("Converting logical sort to streaming sort.")
            .as(Config.class)
            .toRule(RwStreamSort.StreamSortConverterRule.class);

    protected StreamSortConverterRule(Config config) {
      super(config);
    }

    @Override
    public @Nullable RelNode convert(RelNode rel) {
      var rwLogicalSort = (RwLogicalSort) rel;
      var requiredInputTrait = rwLogicalSort.getInput().getTraitSet().replace(STREAMING);
      var newInput = RelOptRule.convert(rwLogicalSort.getInput(), requiredInputTrait);
      return new RwStreamSort(
          rel.getCluster(),
          rwLogicalSort.getTraitSet().plus(STREAMING),
          newInput,
          rwLogicalSort.getCollation(),
          rwLogicalSort.offset,
          rwLogicalSort.fetch);
    }
  }
}
