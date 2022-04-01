package com.risingwave.planner.metadata;

import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMdRowCount;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.BuiltInMethod;
import org.checkerframework.checker.nullness.qual.Nullable;

public class RwRelMdRowCount extends RelMdRowCount {
  public static final RelMetadataProvider SOURCE =
      ReflectiveRelMetadataProvider.reflectiveSource(
          BuiltInMethod.ROW_COUNT.method, new RwRelMdRowCount());

  @Override
  public @Nullable Double getRowCount(Join rel, RelMetadataQuery mq) {
    return rel.estimateRowCount(mq);
  }
}
