package com.risingwave.planner.metadata;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.metadata.ChainedRelMetadataProvider;
import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider;
import org.apache.calcite.rel.metadata.JaninoRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;

public class RisingWaveRelMetadataProvider {
  private static final RelMetadataProvider INSTANCE =
      ChainedRelMetadataProvider.of(ImmutableList.of(DefaultRelMetadataProvider.INSTANCE));

  public static RelMetadataProvider getMetadataProvider() {
    var provider = JaninoRelMetadataProvider.of(INSTANCE);
    RelMetadataQuery.THREAD_PROVIDERS.set(provider);
    return provider;
  }
}
