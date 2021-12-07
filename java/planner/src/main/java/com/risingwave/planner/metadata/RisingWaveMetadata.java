package com.risingwave.planner.metadata;

import java.util.List;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.Metadata;
import org.apache.calcite.rel.metadata.MetadataDef;
import org.apache.calcite.rel.metadata.MetadataHandler;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Define RisingWave's Metadata */
public abstract class RisingWaveMetadata {

  /** PrimaryKey for each operator */
  public interface PrimaryKey extends Metadata {
    MetadataDef<RisingWaveMetadata.PrimaryKey> DEF =
        MetadataDef.of(
            RisingWaveMetadata.PrimaryKey.class,
            RisingWaveMetadata.PrimaryKey.Handler.class,
            Types.lookupMethod(PrimaryKey.class, "getPrimaryKeyIndices"));

    @Nullable
    List<Integer> getPrimaryKeyIndices();

    /** Handler API. */
    interface Handler extends MetadataHandler<RisingWaveMetadata.PrimaryKey> {
      @Nullable
      List<Integer> getPrimaryKeyIndices(RelNode r, RelMetadataQuery mq);
    }
  }
}
