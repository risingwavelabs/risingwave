package com.risingwave.planner.rel.physical.streaming;

import static com.risingwave.planner.rel.logical.RisingWaveLogicalRel.LOGICAL;

import com.risingwave.planner.rel.logical.RwLogicalFilter;
import com.risingwave.planner.rel.serialization.RexToProtoSerializer;
import com.risingwave.proto.streaming.plan.FilterNode;
import com.risingwave.proto.streaming.plan.StreamNode;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rex.RexNode;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Stream Filter */
public class RwStreamFilter extends Filter implements RisingWaveStreamingRel {
  public RwStreamFilter(
      RelOptCluster cluster, RelTraitSet traits, RelNode child, RexNode condition) {
    super(cluster, traits, child, condition);
    checkConvention();
  }

  @Override
  public StreamNode serialize() {
    RexToProtoSerializer rexVisitor = new RexToProtoSerializer();
    FilterNode filterNode =
        FilterNode.newBuilder().setSearchCondition(condition.accept(rexVisitor)).build();
    return StreamNode.newBuilder().setFilterNode(filterNode).build();
  }

  @Override
  public Filter copy(RelTraitSet traitSet, RelNode input, RexNode condition) {
    return new RwStreamFilter(getCluster(), traitSet, input, condition);
  }

  @Override
  public <T> RwStreamingRelVisitor.Result<T> accept(RwStreamingRelVisitor<T> visitor) {
    return visitor.visit(this);
  }

  /** Rule for converting logical filter to stream filter */
  public static class StreamFilterConverterRule extends ConverterRule {
    public static final StreamFilterConverterRule INSTANCE =
        Config.INSTANCE
            .withInTrait(LOGICAL)
            .withOutTrait(STREAMING)
            .withRuleFactory(StreamFilterConverterRule::new)
            .withOperandSupplier(t -> t.operand(RwLogicalFilter.class).anyInputs())
            .withDescription("Converting logical filter to streaming filter.")
            .as(Config.class)
            .toRule(StreamFilterConverterRule.class);

    protected StreamFilterConverterRule(Config config) {
      super(config);
    }

    @Override
    public @Nullable RelNode convert(RelNode rel) {
      var rwLogicalFilter = (RwLogicalFilter) rel;
      var requiredInputTrait = rwLogicalFilter.getInput().getTraitSet().replace(STREAMING);
      var newInput = RelOptRule.convert(rwLogicalFilter.getInput(), requiredInputTrait);
      return new RwStreamFilter(
          rel.getCluster(),
          rwLogicalFilter.getTraitSet().plus(STREAMING),
          newInput,
          rwLogicalFilter.getCondition());
    }
  }
}
