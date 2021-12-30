package com.risingwave.planner.metadata;

import com.risingwave.planner.rel.streaming.PrimaryKeyDerivationVisitor;
import com.risingwave.planner.rel.streaming.RwStreamAgg;
import com.risingwave.planner.rel.streaming.RwStreamBroadcast;
import com.risingwave.planner.rel.streaming.RwStreamChain;
import com.risingwave.planner.rel.streaming.RwStreamExchange;
import com.risingwave.planner.rel.streaming.RwStreamFilter;
import com.risingwave.planner.rel.streaming.RwStreamMaterializedViewSource;
import com.risingwave.planner.rel.streaming.RwStreamProject;
import com.risingwave.planner.rel.streaming.RwStreamSort;
import com.risingwave.planner.rel.streaming.RwStreamTableSource;
import com.risingwave.planner.rel.streaming.join.RwStreamHashJoin;
import java.util.List;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.MetadataDef;
import org.apache.calcite.rel.metadata.MetadataHandler;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.checkerframework.checker.nullness.qual.Nullable;

/** For different operators in streaming convention to get their primary keys */
public class RisingWaveRelMdPrimaryKey implements MetadataHandler<RisingWaveMetadata.PrimaryKey> {
  public static final RelMetadataProvider SOURCE =
      ReflectiveRelMetadataProvider.reflectiveSource(
          Types.lookupMethod(RisingWaveMetadata.PrimaryKey.class, "getPrimaryKeyIndices"),
          new RisingWaveRelMdPrimaryKey());

  private final PrimaryKeyDerivationVisitor visitor;

  protected RisingWaveRelMdPrimaryKey() {
    visitor = new PrimaryKeyDerivationVisitor();
  }

  @Override
  public MetadataDef<RisingWaveMetadata.PrimaryKey> getDef() {
    return RisingWaveMetadata.PrimaryKey.DEF;
  }

  public List<Integer> getPrimaryKeyIndices(RelNode rel, RelMetadataQuery mq) {
    throw new RuntimeException("getPrimaryKeyIndices unimplemented for RelNode");
  }

  public @Nullable List<Integer> getPrimaryKeyIndices(RwStreamAgg rel, RelMetadataQuery mq) {
    return visitor.visit(rel).info.getPrimaryKeyIndices();
  }

  public @Nullable List<Integer> getPrimaryKeyIndices(RwStreamExchange rel, RelMetadataQuery mq) {
    return visitor.visit(rel).info.getPrimaryKeyIndices();
  }

  public @Nullable List<Integer> getPrimaryKeyIndices(RwStreamFilter rel, RelMetadataQuery mq) {
    return visitor.visit(rel).info.getPrimaryKeyIndices();
  }

  public @Nullable List<Integer> getPrimaryKeyIndices(RwStreamSort rel, RelMetadataQuery mq) {
    return visitor.visit(rel).info.getPrimaryKeyIndices();
  }

  public @Nullable List<Integer> getPrimaryKeyIndices(RwStreamProject rel, RelMetadataQuery mq) {
    return visitor.visit(rel).info.getPrimaryKeyIndices();
  }

  public @Nullable List<Integer> getPrimaryKeyIndices(
      RwStreamTableSource rel, RelMetadataQuery mq) {
    return visitor.visit(rel).info.getPrimaryKeyIndices();
  }

  public @Nullable List<Integer> getPrimaryKeyIndices(RwStreamHashJoin rel, RelMetadataQuery mq) {
    return visitor.visit(rel).info.getPrimaryKeyIndices();
  }

  public @Nullable List<Integer> getPrimaryKeyIndices(
      RwStreamMaterializedViewSource rel, RelMetadataQuery mq) {
    return visitor.visit(rel).info.getPrimaryKeyIndices();
  }

  public @Nullable List<Integer> getPrimaryKeyIndices(RwStreamChain rel, RelMetadataQuery mq) {
    return visitor.visit(rel).info.getPrimaryKeyIndices();
  }

  public @Nullable List<Integer> getPrimaryKeyIndices(RwStreamBroadcast rel, RelMetadataQuery mq) {
    return visitor.visit(rel).info.getPrimaryKeyIndices();
  }
}
