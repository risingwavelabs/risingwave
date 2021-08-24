package com.risingwave.planner.rel.physical.batch;

import static com.risingwave.planner.rel.logical.RisingWaveLogicalRel.LOGICAL;

import com.google.protobuf.Any;
import com.risingwave.planner.rel.logical.RwLogicalFilter;
import com.risingwave.planner.rel.serialization.RexToProtoSerializer;
import com.risingwave.proto.plan.FilterNode;
import com.risingwave.proto.plan.PlanNode;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rex.RexNode;
import org.checkerframework.checker.nullness.qual.Nullable;

public class RwBatchFilter extends Filter implements RisingWaveBatchPhyRel {
  protected RwBatchFilter(
      RelOptCluster cluster, RelTraitSet traits, RelNode child, RexNode condition) {
    super(cluster, traits, child, condition);
    checkConvention();
  }

  @Override
  public PlanNode serialize() {
    RexToProtoSerializer rexVisitor = new RexToProtoSerializer();
    FilterNode filter =
        FilterNode.newBuilder().setSearchCondition(condition.accept(rexVisitor)).build();
    return PlanNode.newBuilder()
        .setNodeType(PlanNode.PlanNodeType.FILTER)
        .setBody(Any.pack(filter))
        .addChildren(((RisingWaveBatchPhyRel) input).serialize())
        .build();
  }

  @Override
  public Filter copy(RelTraitSet traitSet, RelNode input, RexNode condition) {
    return new RwBatchFilter(getCluster(), traitSet, input, condition);
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw);
  }

  public static class BatchFilterConverterRule extends ConverterRule {
    public static final BatchFilterConverterRule INSTANCE =
        Config.INSTANCE
            .withInTrait(LOGICAL)
            .withOutTrait(BATCH_PHYSICAL)
            .withRuleFactory(BatchFilterConverterRule::new)
            .withOperandSupplier(t -> t.operand(RwLogicalFilter.class).anyInputs())
            .withDescription("Converting logical filter to batch physical.")
            .as(Config.class)
            .toRule(BatchFilterConverterRule.class);

    protected BatchFilterConverterRule(Config config) {
      super(config);
    }

    @Override
    public @Nullable RelNode convert(RelNode rel) {
      RwLogicalFilter rwLogicalFilter = (RwLogicalFilter) rel;
      RelTraitSet newTraitSet = rwLogicalFilter.getTraitSet().replace(BATCH_PHYSICAL);
      RelNode newInput = RelOptRule.convert(rwLogicalFilter.getInput(), BATCH_PHYSICAL);
      return new RwBatchFilter(
          rel.getCluster(), newTraitSet, newInput, rwLogicalFilter.getCondition());
    }
  }
}
