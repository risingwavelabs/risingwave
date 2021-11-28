package com.risingwave.planner.rel.physical.batch;

import static com.risingwave.planner.rel.logical.RisingWaveLogicalRel.LOGICAL;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.Any;
import com.risingwave.planner.rel.common.dist.RwDistributionTraitDef;
import com.risingwave.planner.rel.logical.RwLogicalFilter;
import com.risingwave.planner.rel.serialization.RexToProtoSerializer;
import com.risingwave.proto.plan.FilterNode;
import com.risingwave.proto.plan.PlanNode;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.PhysicalNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Pair;
import org.checkerframework.checker.nullness.qual.Nullable;

/** physical filter operator */
public class RwBatchFilter extends Filter implements RisingWaveBatchPhyRel, PhysicalNode {
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

  @Override
  public RelNode convertToDistributed() {
    return copy(
        getTraitSet().replace(BATCH_DISTRIBUTED),
        RelOptRule.convert(input, input.getTraitSet().replace(BATCH_DISTRIBUTED)),
        getCondition());
  }

  @Override
  public @Nullable Pair<RelTraitSet, List<RelTraitSet>> passThroughTraits(RelTraitSet required) {
    return null;
  }

  @Override
  public Pair<RelTraitSet, List<RelTraitSet>> deriveTraits(
      final RelTraitSet childTraits, final int childId) {
    if (childTraits.getConvention() != traitSet.getConvention()) {
      return null;
    }
    if (childTraits.getConvention() != BATCH_DISTRIBUTED) {
      return null;
    }

    var newTraits = traitSet;
    var dist = childTraits.getTrait(RwDistributionTraitDef.getInstance());
    if (dist != null) {
      newTraits = newTraits.plus(dist);
    }
    return Pair.of(newTraits, ImmutableList.of(childTraits));
  }

  /** Filter converter rule between logical and physical. */
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
      var rwLogicalFilter = (RwLogicalFilter) rel;
      var requiredInputTrait = rwLogicalFilter.getInput().getTraitSet().plus(BATCH_PHYSICAL);
      var newInput = RelOptRule.convert(rwLogicalFilter.getInput(), requiredInputTrait);
      return new RwBatchFilter(
          rel.getCluster(),
          rwLogicalFilter.getTraitSet().plus(BATCH_PHYSICAL),
          newInput,
          rwLogicalFilter.getCondition());
    }
  }
}
