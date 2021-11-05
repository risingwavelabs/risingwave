package com.risingwave.scheduler.streaming;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.risingwave.scheduler.streaming.graph.StreamGraph;

/** The default implementation of the interface <code>StreamManager</code>. */
@Singleton
public class StreamManagerImpl implements StreamManager {
  private int fragmentId = 0;

  @Inject
  public StreamManagerImpl() {}

  @Override
  public int createFragment() {
    return fragmentId++;
  }

  @Override
  public void scheduleStreamGraph(StreamGraph graph) {}
}
