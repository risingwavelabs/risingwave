package com.risingwave.planner.rel.streaming;

import com.risingwave.catalog.TableCatalog;
import com.risingwave.proto.streaming.plan.ChainNode;
import com.risingwave.proto.streaming.plan.StreamNode;
import com.risingwave.rpc.Messages;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;

/**
 * Chain Node
 *
 * <p>TODO: Chain node should take 2 inputs: a batch plan with a epoch attached, and a broadcast
 * node.
 */
public class RwStreamChain extends SingleRel implements RisingWaveStreamingRel {

  private final TableCatalog.TableId firstTableRefId;

  public RwStreamChain(
      RelOptCluster cluster,
      RelTraitSet traits,
      TableCatalog.TableId firstTableRefId,
      RelNode second) {
    super(cluster, traits, second);
    this.firstTableRefId = firstTableRefId;
    checkConvention();
  }

  /** Explain */
  @Override
  public RelWriter explainTerms(RelWriter pw) {
    var writer = super.explainTerms(pw).item("firstTableRefId", firstTableRefId);
    return writer;
  }

  @Override
  public StreamNode serialize() {
    ChainNode chainNode =
        ChainNode.newBuilder().setTableRefId(Messages.getTableRefId(this.firstTableRefId)).build();
    return StreamNode.newBuilder().setChainNode(chainNode).build();
  }

  @Override
  public <T> RwStreamingRelVisitor.Result<T> accept(RwStreamingRelVisitor<T> visitor) {
    return visitor.visit(this);
  }
}
