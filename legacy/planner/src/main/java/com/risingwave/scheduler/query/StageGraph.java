package com.risingwave.scheduler.query;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.risingwave.common.exception.PgErrorCode;
import com.risingwave.common.exception.PgException;
import com.risingwave.planner.rel.physical.RwBatchExchange;
import com.risingwave.planner.rel.serialization.FragmentWriter;
import com.risingwave.scheduler.stage.QueryStage;
import com.risingwave.scheduler.stage.StageId;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

// A DAG of stages of a query. It's immutable once built.
// TODO: verification of the graph.
class StageGraph {
  private final StageId rootId;
  private final ImmutableMap<StageId, QueryStage> stages;
  private final ImmutableMap<StageId, ImmutableSet<StageId>> childEdges;
  private final ImmutableMap<StageId, ImmutableSet<StageId>> parentEdges;

  // Used to find the child stage linked to an exchange node.
  // Integer is the exchange node's unique ID.
  // StageId is the child stage.
  private final ImmutableMap<Integer, StageId> exchangeIdToStage;

  private StageGraph(
      Map<StageId, QueryStage> stages,
      Map<StageId, Set<StageId>> childEdges,
      Map<StageId, Set<StageId>> parentEdges,
      Map<Integer, StageId> exchangeIdToStage,
      StageId rootId) {
    this.stages = ImmutableMap.copyOf(stages);
    this.childEdges = immutableEdgesOf(childEdges);
    this.parentEdges = immutableEdgesOf(parentEdges);
    this.exchangeIdToStage = ImmutableMap.copyOf(exchangeIdToStage);
    this.rootId = rootId;
  }

  private static ImmutableMap<StageId, ImmutableSet<StageId>> immutableEdgesOf(
      Map<StageId, Set<StageId>> edges) {
    var builder = new ImmutableMap.Builder<StageId, ImmutableSet<StageId>>();
    edges.forEach((id, set) -> builder.put(id, ImmutableSet.copyOf(set)));
    return builder.build();
  }

  static Builder newBuilder() {
    return new Builder();
  }

  StageId getRootStageId() {
    return rootId;
  }

  ImmutableSet<StageId> getParentsChecked(StageId stageId) {
    return Optional.ofNullable(parentEdges.get(stageId))
        .orElseThrow(
            () -> new PgException(PgErrorCode.INTERNAL_ERROR, "Unknown stage id: %s", stageId));
  }

  ImmutableSet<StageId> getChildrenChecked(StageId stageId) {
    return Optional.ofNullable(childEdges.get(stageId))
        .orElseThrow(
            () -> new PgException(PgErrorCode.INTERNAL_ERROR, "Unknown stage id: %s", stageId));
  }

  QueryStage getQueryStageChecked(StageId stageId) {
    Optional<QueryStage> stage = Optional.ofNullable(stages.get(stageId));
    return stage.orElseThrow(
        () -> new PgException(PgErrorCode.INTERNAL_ERROR, "Unknown stage id: %s", stageId));
  }

  ImmutableList<StageId> getLeafStages() {
    var builder = new ImmutableList.Builder<StageId>();
    childEdges.forEach(
        (StageId from, Set<StageId> toSet) -> {
          if (toSet.isEmpty()) {
            builder.add(from);
          }
        });
    return builder.build();
  }

  StageId getExchangeSource(RwBatchExchange node) {
    Optional<StageId> stage = Optional.ofNullable(exchangeIdToStage.get(node.getUniqueId()));
    return stage.orElseThrow(
        () -> new PgException(PgErrorCode.INTERNAL_ERROR, "Unable to find stage by exchange node"));
  }

  @Override
  public String toString() {
    try (var sw = new StringWriter();
        var writer = new PrintWriter(sw)) {
      writer.println("Root stage id: " + rootId);
      for (var stageEntry : stages.entrySet()) {
        writer.println(
            "Children of stage "
                + stageEntry.getKey()
                + " is: "
                + childEdges.get(stageEntry.getKey()));
        writer.println("Plan of stage " + stageEntry.getKey() + " is: ");
        var planWriter = new FragmentWriter(writer);
        stageEntry.getValue().getRoot().explain(planWriter);
      }

      return sw.toString();
    } catch (IOException exp) {
      throw PgException.from(exp);
    }
  }

  static class Builder {
    private final Map<StageId, QueryStage> stages = new HashMap<>();
    private final Map<StageId, Set<StageId>> childEdges = new HashMap<>();
    private final Map<StageId, Set<StageId>> parentEdges = new HashMap<>();
    private final Map<Integer, StageId> exchangeIdToStage = new HashMap<>();

    void linkToChild(StageId parentId, int exchangeId, StageId childId) {
      Set<StageId> childIds = this.childEdges.getOrDefault(parentId, Sets.newHashSet());
      childIds.add(childId);
      this.childEdges.put(parentId, childIds);
      this.exchangeIdToStage.put(exchangeId, childId);
    }

    void addNode(QueryStage stage) {
      stages.put(stage.getStageId(), stage);
    }

    StageGraph build(StageId rootId) {
      stages.forEach(
          (stageId, stage) -> {
            childEdges.computeIfAbsent(stageId, k -> Sets.newHashSet());
            parentEdges.computeIfAbsent(stageId, k -> Sets.newHashSet());
          });

      childEdges.forEach(
          (StageId from, Set<StageId> toSet) ->
              toSet.forEach(
                  (StageId to) -> {
                    var parentSet = parentEdges.getOrDefault(to, Sets.newHashSet());
                    parentSet.add(from);
                    parentEdges.put(to, parentSet);
                  }));

      return new StageGraph(stages, childEdges, parentEdges, exchangeIdToStage, rootId);
    }
  }
}
