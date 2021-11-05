package com.risingwave.scheduler.streaming.graph;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.risingwave.common.exception.PgErrorCode;
import com.risingwave.common.exception.PgException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A <code>StreamStageGraph</code> is analogous to the `StateGraph` in batch plan. It represents the
 * dependency of stages in the stream computation.
 */
public class StreamStageGraph {
  private final int rootStageId;
  private final ImmutableMap<Integer, StreamStage> stages;
  private final ImmutableMap<Integer, ImmutableSet<Integer>> childEdges;
  private final ImmutableMap<Integer, ImmutableSet<Integer>> parentEdges;

  private StreamStageGraph(
      int rootStageId,
      Map<Integer, StreamStage> stages,
      Map<Integer, Set<Integer>> childEdges,
      Map<Integer, Set<Integer>> parentEdges) {

    this.rootStageId = rootStageId;
    this.stages = ImmutableMap.copyOf(stages);
    this.childEdges = immutableEdgesOf(childEdges);
    this.parentEdges = immutableEdgesOf(parentEdges);
  }

  /**
   * Copy edge map to an immutable map.
   *
   * @param edges A mutable map of edges, from a starting node to a mutable set of ending nodes.
   * @return An immutable map of copied values.
   */
  private static ImmutableMap<Integer, ImmutableSet<Integer>> immutableEdgesOf(
      Map<Integer, Set<Integer>> edges) {
    var builder = new ImmutableMap.Builder<Integer, ImmutableSet<Integer>>();
    edges.forEach((id, set) -> builder.put(id, ImmutableSet.copyOf(set)));
    return builder.build();
  }

  public static StageBuilder newBuilder() {
    return new StageBuilder();
  }

  public StreamStage getRootStage() {
    return stages.get(rootStageId);
  }

  public List<StreamStage> getStages() {
    List<StreamStage> list = new ArrayList();
    for (var key : stages.keySet()) {
      list.add(stages.get(key));
    }
    return list;
  }

  public boolean hasDownstream(int stageId) {
    return childEdges.containsKey(stageId);
  }

  public ImmutableSet<Integer> getDownstreamStages(int stageId) {
    if (childEdges.containsKey(stageId)) {
      return childEdges.get(stageId);
    } else {
      throw new PgException(PgErrorCode.INTERNAL_ERROR, "Cannot find the stage " + stageId);
    }
  }

  public StreamStage getStageById(int stageId) {
    if (stages.containsKey(stageId)) {
      return stages.get(stageId);
    } else {
      throw new PgException(PgErrorCode.INTERNAL_ERROR, "Cannot find the stage " + stageId);
    }
  }

  /** Building immutable stream stage graph. */
  public static class StageBuilder {
    private final Map<Integer, StreamStage> stages = new HashMap<>();
    // Downstream are parents.
    private final Map<Integer, Set<Integer>> childEdges = new HashMap<>();
    private final Map<Integer, Set<Integer>> parentEdges = new HashMap<>();

    /** Add stage to the builder. */
    public void addStage(StreamStage stage) {
      stages.put(stage.getStageId(), stage);
    }

    /** Add link between stages. */
    public void linkToChild(int parentId, int childId) {
      Set<Integer> children = childEdges.getOrDefault(parentId, Sets.newHashSet());
      Set<Integer> parents = parentEdges.getOrDefault(childId, Sets.newHashSet());
      children.add(childId);
      parents.add(parentId);
      this.childEdges.put(parentId, children);
      this.parentEdges.put(childId, parents);
    }

    /** Build immutable streaming stage graph. */
    public StreamStageGraph build(int rootId) {
      return new StreamStageGraph(rootId, stages, childEdges, parentEdges);
    }
  }
}
