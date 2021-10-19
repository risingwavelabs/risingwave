package com.risingwave.planner.rel.physical.streaming;

import static com.risingwave.planner.rel.logical.RisingWaveLogicalRel.LOGICAL;

import com.google.protobuf.Any;
import com.risingwave.planner.rel.logical.RwLogicalAggregate;
import com.risingwave.planner.rel.physical.RwAggregate;
import com.risingwave.proto.expr.InputRefExpr;
import com.risingwave.proto.streaming.plan.HashAggNode;
import com.risingwave.proto.streaming.plan.SimpleAggNode;
import com.risingwave.proto.streaming.plan.StreamNode;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.util.ImmutableBitSet;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Stream Aggregation */
public class RwStreamAgg extends RwAggregate implements RisingWaveStreamingRel {
  public RwStreamAgg(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      List<RelHint> hints,
      RelNode input,
      ImmutableBitSet groupSet,
      @Nullable List<ImmutableBitSet> groupSets,
      List<AggregateCall> aggCalls) {
    super(cluster, traitSet, hints, input, groupSet, groupSets, aggCalls);
    checkConvention();
  }

  @Override
  public StreamNode serialize() {
    StreamNode node;
    if (groupSet.length() == 0) {
      SimpleAggNode.Builder simpleAggNodeBuilder = SimpleAggNode.newBuilder();
      for (AggregateCall aggCall : aggCalls) {
        simpleAggNodeBuilder.addAggCalls(serializeAggCall(aggCall));
      }
      node =
          StreamNode.newBuilder()
              .setNodeType(StreamNode.StreamNodeType.LOCAL_SIMPLE_AGG)
              .setBody(Any.pack(simpleAggNodeBuilder.build()))
              .setInput(((RisingWaveStreamingRel) input).serialize())
              .build();
    } else {
      HashAggNode.Builder hashAggNodeBuilder = HashAggNode.newBuilder();
      for (int i = groupSet.nextSetBit(0); i >= 0; i = groupSet.nextSetBit(i + 1)) {
        hashAggNodeBuilder.addGroupKeys(InputRefExpr.newBuilder().setColumnIdx(i).build());
      }
      for (AggregateCall aggCall : aggCalls) {
        hashAggNodeBuilder.addAggCalls(serializeAggCall(aggCall));
      }
      node =
          StreamNode.newBuilder()
              .setNodeType(StreamNode.StreamNodeType.LOCAL_HASH_AGG)
              .setBody(Any.pack(hashAggNodeBuilder.build()))
              .setInput(((RisingWaveStreamingRel) input).serialize())
              .build();
    }
    return node;
  }

  @Override
  public Aggregate copy(
      RelTraitSet traitSet,
      RelNode input,
      ImmutableBitSet groupSet,
      @Nullable List<ImmutableBitSet> groupSets,
      List<AggregateCall> aggCalls) {
    return new RwStreamAgg(
        getCluster(), traitSet, getHints(), input, groupSet, groupSets, aggCalls);
  }

  /** Rule for converting logical aggregation to stream aggregation */
  public static class StreamAggregationConverterRule extends ConverterRule {
    public static final RwStreamAgg.StreamAggregationConverterRule INSTANCE =
        ConverterRule.Config.INSTANCE
            .withInTrait(LOGICAL)
            .withOutTrait(STREAMING)
            .withRuleFactory(RwStreamAgg.StreamAggregationConverterRule::new)
            .withOperandSupplier(t -> t.operand(RwLogicalAggregate.class).anyInputs())
            .withDescription("Converting logical agg to streaming agg.")
            .as(ConverterRule.Config.class)
            .toRule(RwStreamAgg.StreamAggregationConverterRule.class);

    protected StreamAggregationConverterRule(Config config) {
      super(config);
    }

    @Override
    public @Nullable RelNode convert(RelNode rel) {
      RwLogicalAggregate rwLogicalAggregate = (RwLogicalAggregate) rel;
      RelTraitSet requiredInputTraits =
          rwLogicalAggregate.getInput().getTraitSet().replace(STREAMING);
      RelNode newInput = RelOptRule.convert(rwLogicalAggregate.getInput(), requiredInputTraits);

      return new RwStreamAgg(
          rwLogicalAggregate.getCluster(),
          rwLogicalAggregate.getTraitSet().plus(STREAMING),
          rwLogicalAggregate.getHints(),
          newInput,
          rwLogicalAggregate.getGroupSet(),
          rwLogicalAggregate.getGroupSets(),
          rwLogicalAggregate.getAggCallList());
    }
  }
}
