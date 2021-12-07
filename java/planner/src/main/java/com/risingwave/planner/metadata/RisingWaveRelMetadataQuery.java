package com.risingwave.planner.metadata;

import java.util.List;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.CyclicMetadataException;
import org.apache.calcite.rel.metadata.JaninoRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Customized RisingWaveRelMetadataQuery */
public class RisingWaveRelMetadataQuery extends RelMetadataQuery {

  private RisingWaveMetadata.PrimaryKey.Handler primaryKeyHandler;

  public RisingWaveRelMetadataQuery() {
    super();
    this.primaryKeyHandler = initialHandler(RisingWaveMetadata.PrimaryKey.Handler.class);
  }

  public @Nullable List<Integer> getPrimaryKeyIndices(RelNode rel) {
    for (; ; ) {
      try {
        return this.primaryKeyHandler.getPrimaryKeyIndices(rel, this);
      } catch (JaninoRelMetadataProvider.NoHandler e) {
        this.primaryKeyHandler = revise(e.relClass, RisingWaveMetadata.PrimaryKey.DEF);
      } catch (CyclicMetadataException e) {
        return null;
      }
    }
  }

  public static RisingWaveRelMetadataQuery instance() {
    return new RisingWaveRelMetadataQuery();
  }
}
