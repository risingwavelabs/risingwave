package com.risingwave.planner.rel.streaming;

import static com.google.common.base.Verify.verify;

import com.risingwave.catalog.ColumnDesc;
import com.risingwave.catalog.ColumnEncoding;
import com.risingwave.common.datatype.RisingWaveDataType;
import com.risingwave.planner.metadata.RisingWaveRelMetadataQuery;
import com.risingwave.planner.rel.common.dist.RwDistributionTrait;
import com.risingwave.proto.streaming.plan.MergeNode;
import com.risingwave.proto.streaming.plan.StreamNode;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Exchange;

/** The exchange node in a streaming plan. */
public class RwStreamExchange extends Exchange implements RisingWaveStreamingRel {

  /**
   * The upstream fragments of the exchange node should be added in <code>buildFragmentsInStage
   * </code>
   */
  private final Set<Integer> upstreamSet = new HashSet<>();

  public RwStreamExchange(
      RelOptCluster cluster, RelTraitSet traitSet, RelNode input, RelDistribution distribution) {
    super(cluster, traitSet, input, distribution);
    checkConvention();
    verify(
        traitSet.contains(distribution), "Trait set: %s, distribution: %s", traitSet, distribution);
  }

  @Override
  public StreamNode serialize() {
    var primaryKeyIndices =
        ((RisingWaveRelMetadataQuery) getCluster().getMetadataQuery()).getPrimaryKeyIndices(this);
    var mergerBuilder = MergeNode.newBuilder();
    this.upstreamSet.forEach(mergerBuilder::addUpstreamFragmentId);
    for (ColumnDesc columnDesc : this.getSchema()) {
      com.risingwave.proto.plan.ColumnDesc.Builder columnDescBuilder =
          com.risingwave.proto.plan.ColumnDesc.newBuilder();
      columnDescBuilder
          .setEncoding(com.risingwave.proto.plan.ColumnDesc.ColumnEncodingType.RAW)
          .setColumnType(columnDesc.getDataType().getProtobufType())
          .setIsPrimary(columnDesc.isPrimary());
      mergerBuilder.addInputColumnDescs(columnDescBuilder.build());
    }
    var mergeNode = mergerBuilder.build();
    return StreamNode.newBuilder()
        .setMergeNode(mergeNode)
        .addAllPkIndices(primaryKeyIndices)
        .build();
  }

  @Override
  public Exchange copy(RelTraitSet traitSet, RelNode newInput, RelDistribution newDistribution) {
    return new RwStreamExchange(getCluster(), traitSet, newInput, newDistribution);
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

  public void addUpStream(int upstreamFragmentId) {
    upstreamSet.add(upstreamFragmentId);
  }

  public Set<Integer> getUpstreamSet() {
    return upstreamSet;
  }

  private List<ColumnDesc> getSchema() {
    // Add every column from its upstream root node.
    // Here root node would suffice as the streaming plan is still reversed.
    // E.g. Source -> Filter -> Proj. The root will be project and the schema of project is
    // what we needed.
    var rowType = this.getRowType();
    List<ColumnDesc> schema = new ArrayList<>();
    for (int i = 0; i < rowType.getFieldCount(); i++) {
      var field = rowType.getFieldList().get(i);
      ColumnDesc columnDesc =
          new ColumnDesc((RisingWaveDataType) field.getType(), false, ColumnEncoding.RAW);
      schema.add(columnDesc);
    }
    return schema;
  }

  public static RwStreamExchange create(RelNode input, RwDistributionTrait distribution) {
    RelOptCluster cluster = input.getCluster();
    RelTraitSet traitSet = input.getTraitSet().plus(STREAMING).plus(distribution);
    var dist = traitSet.canonize(distribution);
    return new RwStreamExchange(cluster, traitSet, input, dist);
  }

  @Override
  public <T> RwStreamingRelVisitor.Result<T> accept(RwStreamingRelVisitor<T> visitor) {
    return visitor.visit(this);
  }
}
