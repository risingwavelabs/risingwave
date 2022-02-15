package com.risingwave.planner.rel.physical;

import static com.google.common.base.Verify.verify;

import com.google.common.collect.ImmutableList;
import com.risingwave.common.datatype.RisingWaveDataType;
import com.risingwave.planner.rel.common.dist.RwDistributionTrait;
import com.risingwave.proto.data.DataType;
import com.risingwave.proto.expr.InputRefExpr;
import com.risingwave.proto.plan.ColumnOrder;
import com.risingwave.proto.plan.ExchangeNode;
import com.risingwave.proto.plan.MergeSortExchangeNode;
import com.risingwave.proto.plan.OrderType;
import com.risingwave.proto.plan.PlanNode;
import com.risingwave.scheduler.exchange.BroadcastDistribution;
import com.risingwave.scheduler.exchange.Distribution;
import com.risingwave.scheduler.exchange.HashDistribution;
import com.risingwave.scheduler.exchange.SingleDistribution;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rex.RexInputRef;
import org.apache.commons.lang3.SerializationException;

/** Exchange Operator in Batch convention */
public class RwBatchExchange extends Exchange implements RisingWaveBatchPhyRel {
  private final int uniqueId;

  private RwBatchExchange(
      RelOptCluster cluster, RelTraitSet traitSet, RelNode input, RelDistribution distribution) {
    super(cluster, traitSet, input, distribution);
    checkConvention();
    verify(
        traitSet.contains(distribution), "Trait set: %s, distribution: %s", traitSet, distribution);
    verify(traitSet.contains(BATCH_DISTRIBUTED), "Trait set: %s", traitSet);

    this.uniqueId = super.getId();
  }

  @Override
  public Exchange copy(RelTraitSet traitSet, RelNode newInput, RelDistribution newDistribution) {
    return new RwBatchExchange(getCluster(), traitSet, newInput, newDistribution);
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    var writer = super.explainTerms(pw);
    var collation = getTraitSet().getCollation();
    if (collation != null) {
      writer.item("collation", collation);
    }

    return writer;
  }

  @Override
  public PlanNode serialize() {
    // Exchange is a pipeline breaker, we do not serialize its children.
    // The exchange sources is unknown by far, which will be assigned later by scheduler.
    RelCollation exchangeCollation = this.getTraitSet().getCollation();
    RelCollation inputCollation = this.getInput().getTraitSet().getCollation();
    // Only if the inputCollation is equal or stricter than exchangeCollation,
    // then we can use MergeSortExchange as MergeSortExchange will only preserve the inputCollation
    if (!exchangeCollation.getFieldCollations().isEmpty()
        && inputCollation.satisfies(exchangeCollation)) {
      // Without collation, we don't need to merge sort, a generic exchange is enough
      List<ColumnOrder> columnOrders = new ArrayList<ColumnOrder>();
      List<RelFieldCollation> rfc = exchangeCollation.getFieldCollations();
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
      MergeSortExchangeNode.Builder builder = MergeSortExchangeNode.newBuilder();
      builder.addAllColumnOrders(columnOrders);
      return PlanNode.newBuilder()
          .setMergeSortExchange(builder.build())
          .setIdentity(BatchPlan.getCurrentNodeIdentity(this))
          .build();
    } else {
      return PlanNode.newBuilder()
          .setExchange(ExchangeNode.newBuilder().build())
          .setIdentity(BatchPlan.getCurrentNodeIdentity(this))
          .build();
    }
  }

  public static RwBatchExchange create(RelNode input, RwDistributionTrait distribution) {
    RelOptCluster cluster = input.getCluster();
    RelTraitSet traitSet = input.getTraitSet().plus(BATCH_DISTRIBUTED).plus(distribution);
    return new RwBatchExchange(cluster, traitSet, input, distribution);
  }

  @Override
  public boolean isEnforcer() {
    return true;
  }

  // Every call will return the same id.
  public int getUniqueId() {
    return uniqueId;
  }

  public Distribution createDistribution() {
    var distributionType = this.distribution.getType();
    if (distributionType == RelDistribution.Type.SINGLETON) {
      return new SingleDistribution();
    } else if (distributionType == RelDistribution.Type.BROADCAST_DISTRIBUTED) {
      return new BroadcastDistribution();
    } else if (distributionType == RelDistribution.Type.HASH_DISTRIBUTED) {
      return new HashDistribution(ImmutableList.copyOf(this.distribution.getKeys()));
    }
    throw new UnsupportedOperationException("Unsupported DistributionSchema");
  }
}
