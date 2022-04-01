package com.risingwave.planner.rules.distributed.join;

import static com.risingwave.planner.rel.physical.RisingWaveBatchPhyRel.BATCH_DISTRIBUTED;
import static com.risingwave.planner.rel.physical.RisingWaveBatchPhyRel.BATCH_PHYSICAL;
import static com.risingwave.planner.rel.physical.join.BatchJoinUtils.isEquiJoin;

import com.risingwave.planner.rel.common.dist.RwDistributions;
import com.risingwave.planner.rel.physical.join.RwBufferJoinBase;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.util.ImmutableIntList;
import org.checkerframework.checker.nullness.qual.Nullable;
// FIXME: not guaranteed for SEMI/ANTI join(broadcast which side?)
// FIXME: guess not correct for null-aware ANTI join

/** Rule converting a EquiJoin to distributed version shard by equal key */
public class ShuffleJoinRule extends ConverterRule {
  public static final ShuffleJoinRule INSTANCE =
      Config.INSTANCE
          .withInTrait(BATCH_PHYSICAL)
          .withOutTrait(BATCH_DISTRIBUTED)
          .withRuleFactory(ShuffleJoinRule::new)
          .withOperandSupplier(t -> t.operand(RwBufferJoinBase.class).anyInputs())
          .withDescription("convert to shuffle join.")
          .as(Config.class)
          .toRule(ShuffleJoinRule.class);

  protected ShuffleJoinRule(Config config) {
    super(config);
  }

  private static RelNode transformInput(RelNode input, ImmutableIntList keys) {
    var requiredTraits =
        input.getTraitSet().plus(BATCH_DISTRIBUTED).plus(RwDistributions.hash(keys));
    return RelOptRule.convert(input, requiredTraits);
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    var join = (RwBufferJoinBase) call.rel(0);
    var joinInfo = join.analyzeCondition();
    return isEquiJoin(joinInfo);
  }

  @Override
  public @Nullable RelNode convert(RelNode rel) {
    var join = (RwBufferJoinBase) rel;

    var joinInfo = join.analyzeCondition();

    var newLeft = transformInput(join.getLeft(), joinInfo.leftKeys);
    var newRight = transformInput(join.getRight(), joinInfo.rightKeys);

    // other trait will generate by derive method of join
    return join.copy(
        join.getTraitSet().plus(BATCH_DISTRIBUTED),
        join.getCondition(),
        newLeft,
        newRight,
        join.getJoinType(),
        join.isSemiJoinDone());
  }
}
