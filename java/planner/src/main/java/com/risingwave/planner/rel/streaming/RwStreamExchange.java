package com.risingwave.planner.rel.streaming;

import static com.google.common.base.Verify.verify;

import com.risingwave.common.datatype.RisingWaveDataType;
import com.risingwave.planner.metadata.RisingWaveRelMetadataQuery;
import com.risingwave.planner.rel.common.dist.RwDistributionTrait;
import com.risingwave.proto.plan.Field;
import com.risingwave.proto.streaming.plan.DispatchStrategy;
import com.risingwave.proto.streaming.plan.DispatcherType;
import com.risingwave.proto.streaming.plan.ExchangeNode;
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
   * The upstream fragments of the exchange node should be added in <code>BuildActorssInStage
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
    this.upstreamSet.forEach(mergerBuilder::addUpstreamActorId);
    mergerBuilder.addAllFields(this.getFields());

    var mergeNode = mergerBuilder.build();

    return StreamNode.newBuilder()
        .setMergeNode(mergeNode)
        .addAllPkIndices(primaryKeyIndices)
        .setIdentity(StreamingPlan.getCurrentNodeIdentity(this))
        .build();
  }

  public StreamNode serializeExchange() {
    var exchangeBuilder = ExchangeNode.newBuilder();
    var primaryKeyIndices =
        ((RisingWaveRelMetadataQuery) getCluster().getMetadataQuery()).getPrimaryKeyIndices(this);
    exchangeBuilder.addAllFields(this.getFields());

    // Add dispatcher.
    RelDistribution distribution = getDistribution();
    DispatchStrategy.Builder dispatcherBuilder = DispatchStrategy.newBuilder();
    if (distribution.getType() == RelDistribution.Type.BROADCAST_DISTRIBUTED) {
      dispatcherBuilder.setType(DispatcherType.BROADCAST);
    } else if (distribution.getType() == RelDistribution.Type.HASH_DISTRIBUTED) {
      dispatcherBuilder.setType(DispatcherType.HASH);
      dispatcherBuilder.addAllColumnIndices(distribution.getKeys());
    } else if (distribution.getType() == RelDistribution.Type.SINGLETON) {
      dispatcherBuilder.setType(DispatcherType.SIMPLE);
    }
    exchangeBuilder.setStrategy(dispatcherBuilder);

    return StreamNode.newBuilder()
        .setExchangeNode(exchangeBuilder)
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

  public void addUpStream(int upstreamActorId) {
    upstreamSet.add(upstreamActorId);
  }

  public Set<Integer> getUpstreamSet() {
    return upstreamSet;
  }

  private List<Field> getFields() {
    // Add every column from its upstream root node.
    // Here root node would suffice as the streaming plan is still reversed.
    // E.g. Source -> Filter -> Proj. The root will be project and the schema of project is
    // what we needed.
    var rowType = this.getRowType();
    List<Field> list = new ArrayList<>();
    for (int i = 0; i < rowType.getFieldCount(); i++) {
      var field = rowType.getFieldList().get(i);
      var dataType = (RisingWaveDataType) field.getType();
      list.add(
          Field.newBuilder()
              .setDataType(dataType.getProtobufType())
              .setName(field.getName())
              .build());
    }
    return list;
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
