package com.risingwave.planner.rel.streaming;

import com.risingwave.catalog.TableCatalog;
import com.risingwave.proto.streaming.plan.ChainNode;
import com.risingwave.proto.streaming.plan.StreamNode;
import com.risingwave.rpc.Messages;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.hint.RelHint;

/**
 * Chain Node
 *
 * <p>TODO: Chain node should take 2 inputs: a batch plan with a epoch attached, and a broadcast
 * node.
 */
public class RwStreamChain extends TableScan implements RisingWaveStreamingRel {

  private final TableCatalog.TableId tableId;

  public RwStreamChain(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      List<RelHint> hints,
      RelOptTable table,
      TableCatalog.TableId tableId) {
    super(cluster, traitSet, hints, table);
    this.tableId = tableId;
  }

  /** Explain */
  @Override
  public RelWriter explainTerms(RelWriter pw) {
    var writer = super.explainTerms(pw).item("tableId", tableId);
    return writer;
  }

  @Override
  public StreamNode serialize() {
    ChainNode chainNode =
        ChainNode.newBuilder().setTableRefId(Messages.getTableRefId(tableId)).build();
    return StreamNode.newBuilder().setChainNode(chainNode).build();
  }

  @Override
  public <T> RwStreamingRelVisitor.Result<T> accept(RwStreamingRelVisitor<T> visitor) {
    return visitor.visit(this);
  }
}
