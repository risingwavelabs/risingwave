package com.risingwave.planner.rel.physical;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.collect.ImmutableMap;
import com.risingwave.common.datatype.RisingWaveDataType;
import com.risingwave.planner.rel.common.dist.RwDistributionTrait;
import com.risingwave.planner.rel.common.dist.RwDistributions;
import com.risingwave.proto.expr.AggCall;
import com.risingwave.proto.expr.InputRefExpr;
import java.util.Collections;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.ImmutableBitSet;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Base class for Aggregation */
public abstract class RwAggregate extends Aggregate {

  private static final ImmutableMap<SqlKind, AggCall.Type> SQL_TO_AGG_CALL =
      ImmutableMap.<SqlKind, AggCall.Type>builder()
          .put(SqlKind.SUM, AggCall.Type.SUM)
          // `SUM0` is the global phase for `COUNT`. It is different from `SUM` for type inference
          // rules but can be treated the same in backend.
          .put(SqlKind.SUM0, AggCall.Type.SUM)
          .put(SqlKind.COUNT, AggCall.Type.COUNT)
          .put(SqlKind.MIN, AggCall.Type.MIN)
          .put(SqlKind.MAX, AggCall.Type.MAX)
          .put(SqlKind.SINGLE_VALUE, AggCall.Type.SINGLE_VALUE)
          .build();

  /** derive Distribution trait for agg from input side. */
  public static RwDistributionTrait aggDistributionDerive(
      RwAggregate agg, RwDistributionTrait inputDistribution) {
    if (inputDistribution.getType() == RelDistribution.Type.HASH_DISTRIBUTED) {
      var disKeys = inputDistribution.getKeys();
      if (disKeys.size() != 0 && Collections.max(disKeys) >= agg.getGroupCount()) {
        return RwDistributions.RANDOM_DISTRIBUTED;
      }
    }
    return inputDistribution;
  }

  public RwAggregate(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      List<RelHint> hints,
      RelNode input,
      ImmutableBitSet groupSet,
      @Nullable List<ImmutableBitSet> groupSets,
      List<AggregateCall> aggCalls) {
    super(cluster, traitSet, hints, input, groupSet, groupSets, aggCalls);
  }

  public int[] getGroupKeys() {
    return groupSet.toArray();
  }

  protected AggCall serializeAggCall(AggregateCall call) {
    checkArgument(call.getAggregation().kind != SqlKind.AVG, "avg is not yet supported");
    var builder = AggCall.newBuilder();
    builder.setType(SQL_TO_AGG_CALL.get(call.getAggregation().kind));
    builder.setReturnType(((RisingWaveDataType) call.getType()).getProtobufType());
    builder.setDistinct(call.isDistinct());
    for (int column : call.getArgList()) {
      var type = input.getRowType().getFieldList().get(column).getType();
      var arg =
          AggCall.Arg.newBuilder()
              .setInput(InputRefExpr.newBuilder().setColumnIdx(column).build())
              .setType(((RisingWaveDataType) type).getProtobufType())
              .build();
      builder.addArgs(arg);
    }
    return builder.build();
  }
}
