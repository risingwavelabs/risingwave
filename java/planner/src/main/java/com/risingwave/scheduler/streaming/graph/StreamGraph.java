package com.risingwave.scheduler.streaming.graph;

import com.google.common.collect.ImmutableMap;
import com.risingwave.common.exception.PgErrorCode;
import com.risingwave.common.exception.PgException;
import com.risingwave.planner.rel.streaming.RisingWaveStreamingRel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A <code>StreamGraph</code> represents an actor DAG for a streaming job. Each stage in the stage
 * graph will be translated into multiple parallel <code>StreamFragment</code>s to be executed in
 * parallel or distributed mode.
 */
public class StreamGraph {
  private final ImmutableMap<Integer, StreamFragment> fragments;

  private StreamGraph(Map<Integer, StreamFragment> fragments) {
    this.fragments = ImmutableMap.copyOf(fragments);
  }

  public static GraphBuilder newBuilder() {
    return new GraphBuilder();
  }

  public StreamFragment getFragmentById(int id) {
    if (fragments.containsKey(id)) {
      return fragments.get(id);
    } else {
      throw new PgException(PgErrorCode.INTERNAL_ERROR, "Cannot find the fragment " + id);
    }
  }

  public List<StreamFragment> getAllFragments() {
    List<StreamFragment> list = new ArrayList(fragments.values());
    return list;
  }

  public List<Integer> getAllActorIds() {
    List<Integer> list = new ArrayList<>();
    for (var actor : fragments.values()) {
      list.add(actor.getId());
    }
    return list;
  }

  /** The builder class of a <code>StreamGraph</code>. */
  public static class GraphBuilder {
    private final Map<Integer, StreamFragment> fragments = new HashMap<>();

    public GraphBuilder() {}

    public void addFragment(int id, StreamFragment fragment) {
      fragments.put(id, fragment);
    }

    public void addDependency(int upstreamStageId, int upstream, int downstream) {
      StreamFragment upFragment = fragments.get(upstream);
      StreamFragment.FragmentBuilder upBuilder = upFragment.toBuilder();
      upBuilder.addDownstream(downstream);
      fragments.put(upstream, upBuilder.build());

      StreamFragment downFragment = fragments.get(downstream);
      StreamFragment.FragmentBuilder downBuilder = downFragment.toBuilder();
      downBuilder.addUpstream(upstreamStageId, upstream);
      fragments.put(downstream, downBuilder.build());
    }

    public StreamGraph build() {
      // Add upstream input schema for each fragment.
      for (StreamFragment fragment : fragments.values()) {
        // For now we do not consider join. In this case we extract the schema from the output of
        // the first upstream fragment.
        if (fragment.getUpstreamSets().size() > 0) {
          // A lengthy expression to retrieve an arbitrary (0) upstream fragment.
          int upStreamExampleId = fragment.getUpstreamSets().get(0).right.asList().get(0);
          StreamFragment upStreamExampleFragment = fragments.get(upStreamExampleId);
          RisingWaveStreamingRel exampleRoot = upStreamExampleFragment.getRoot();
          fragments.put(fragment.getId(), fragment);
        }
      }
      // Build stream graph.
      return new StreamGraph(fragments);
    }
  }
}
