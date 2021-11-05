package com.risingwave.scheduler.streaming;

import com.risingwave.scheduler.streaming.graph.StreamGraph;

/** The interface for managing stream actors among distributed compute nodes. */
public interface StreamManager {
  int createFragment();

  void scheduleStreamGraph(StreamGraph graph);
}
