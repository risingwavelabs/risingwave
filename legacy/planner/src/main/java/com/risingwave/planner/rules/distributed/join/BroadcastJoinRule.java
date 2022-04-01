package com.risingwave.planner.rules.distributed.join;

import static com.risingwave.planner.rel.physical.RisingWaveBatchPhyRel.BATCH_DISTRIBUTED;
import static com.risingwave.planner.rel.physical.RisingWaveBatchPhyRel.BATCH_PHYSICAL;
import static com.risingwave.planner.rel.physical.join.BatchJoinUtils.isEquiJoin;

import com.risingwave.planner.rel.common.dist.RwDistributions;
import com.risingwave.planner.rel.physical.join.RwBufferJoinBase;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.checkerframework.checker.nullness.qual.Nullable;
// FIXME: not guaranteed for SEMI/ANTI join(broadcast which side?)
// FIXME: guess not correct for null-aware ANTI join

/** Rule converting a join to broadcast one side distributed version */
public class BroadcastJoinRule extends ConverterRule {
  public static final BroadcastJoinRule INSTANCE =
      Config.INSTANCE
          .withInTrait(BATCH_PHYSICAL)
          .withOutTrait(BATCH_DISTRIBUTED)
          .withRuleFactory(BroadcastJoinRule::new)
          .withOperandSupplier(t -> t.operand(RwBufferJoinBase.class).anyInputs())
          .withDescription("convert to broadcast join.")
          .as(Config.class)
          .toRule(BroadcastJoinRule.class);

  protected BroadcastJoinRule(Config config) {
    super(config);
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    var join = (RwBufferJoinBase) call.rel(0);
    var joinInfo = join.analyzeCondition();
    // FIXME: currently cardinality estimation is inaccurate without enough statistics to determine
    // shuffleHashJoin or broadcastHashJoin
    return !isEquiJoin(joinInfo);
  }

  @Override
  public @Nullable RelNode convert(RelNode rel) {
    var join = (RwBufferJoinBase) rel;
    var joinType = join.getJoinType();
    var left = join.getLeft();
    var leftTraits = left.getTraitSet().plus(BATCH_DISTRIBUTED);
    var right = join.getRight();
    var rightTraits = right.getTraitSet().plus(BATCH_DISTRIBUTED);
    var joinTraits = join.getTraitSet().plus(BATCH_DISTRIBUTED);
    if (joinType.generatesNullsOnLeft() && joinType.generatesNullsOnRight()) {
      // full outer, stand alone
      leftTraits = leftTraits.plus(RwDistributions.SINGLETON);
      rightTraits = rightTraits.plus(RwDistributions.SINGLETON);
      joinTraits = joinTraits.plus(RwDistributions.SINGLETON);
    } else if (joinType.generatesNullsOnLeft()) {
      // right outer, broadcast left, shard right
      leftTraits = leftTraits.plus(RwDistributions.BROADCAST_DISTRIBUTED);
      rightTraits = rightTraits.plus(RwDistributions.RANDOM_DISTRIBUTED);
      joinTraits = joinTraits.plus(RwDistributions.RANDOM_DISTRIBUTED);
    } else {
      // broadcast right(smaller), shard left
      leftTraits = leftTraits.plus(RwDistributions.RANDOM_DISTRIBUTED);
      rightTraits = rightTraits.plus(RwDistributions.BROADCAST_DISTRIBUTED);
      joinTraits = joinTraits.plus(RwDistributions.RANDOM_DISTRIBUTED);
    }

    leftTraits = leftTraits.simplify();
    rightTraits = rightTraits.simplify();
    var newLeft = convert(left, leftTraits);
    var newRight = convert(right, rightTraits);
    // other trait will generate by derive method of join
    return join.copy(
        joinTraits, join.getCondition(), newLeft, newRight, joinType, join.isSemiJoinDone());
  }
}
