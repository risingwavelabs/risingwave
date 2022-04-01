package com.risingwave.planner.rel.physical;

import static java.util.Objects.requireNonNull;

import com.risingwave.planner.rel.common.IdentityExtractor;
import com.risingwave.proto.plan.ExchangeInfo;
import com.risingwave.proto.plan.PlanFragment;
import com.risingwave.proto.plan.PlanNode;
import org.apache.calcite.rel.RelNode;

/**
 * Plan for adhoc query execution. The counterpart is streaming plan, which is designed for
 * streaming execution.
 */
public class BatchPlan {
  private final RisingWaveBatchPhyRel root;

  public BatchPlan(RisingWaveBatchPhyRel root) {
    this.root = requireNonNull(root, "Root can't be null!");
  }

  public RisingWaveBatchPhyRel getRoot() {
    return root;
  }

  public PlanFragment serialize() {
    ExchangeInfo exchangeInfo =
        ExchangeInfo.newBuilder().setMode(ExchangeInfo.DistributionMode.SINGLE).build();

    PlanNode rootNode = root.serialize();

    return PlanFragment.newBuilder().setRoot(rootNode).setExchangeInfo(exchangeInfo).build();
  }

  public static String getCurrentNodeIdentity(RelNode node) {
    return IdentityExtractor.getCurrentNodeIdentity(node, "RwBatch");
  }
}
