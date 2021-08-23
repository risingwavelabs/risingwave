package com.risingwave.planner.rel.physical.batch;

import static com.risingwave.planner.rel.logical.RisingWaveLogicalRel.LOGICAL;

import com.google.protobuf.Any;
import com.risingwave.planner.rel.logical.RwFilter;
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

public class BatchFilter extends Filter implements RisingWaveBatchPhyRel {
  protected BatchFilter(
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
    return new BatchFilter(getCluster(), traitSet, input, condition);
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
            .withOperandSupplier(t -> t.operand(RwFilter.class).anyInputs())
            .withDescription("Converting logical filter to batch physical.")
            .as(Config.class)
            .toRule(BatchFilterConverterRule.class);

    protected BatchFilterConverterRule(Config config) {
      super(config);
    }

    @Override
    public @Nullable RelNode convert(RelNode rel) {
      RwFilter rwFilter = (RwFilter) rel;
      RelTraitSet newTraitSet = rwFilter.getTraitSet().replace(BATCH_PHYSICAL);
      RelNode newInput = RelOptRule.convert(rwFilter.getInput(), BATCH_PHYSICAL);
      return new BatchFilter(rel.getCluster(), newTraitSet, newInput, rwFilter.getCondition());
    }
  }
}
