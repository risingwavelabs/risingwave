package com.risingwave.scheduler.streaming;

import com.risingwave.scheduler.streaming.graph.StreamGraph;
import java.util.List;

/** The interface for managing stream actors among distributed compute nodes. */
public interface StreamManager {
  int createFragment();

  List<StreamRequest> scheduleStreamGraph(StreamGraph graph);

  String nextScheduleId();

  ActorInfoTable getActorInfo(List<Integer> actorIdList);
}
