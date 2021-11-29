package com.risingwave.planner.rel.streaming;

import com.risingwave.planner.rel.streaming.join.RwStreamHashJoin;
import javax.annotation.Nonnull;

/** Visitor for nodes in streaming convention */
public interface RwStreamingRelVisitor<T> {

  Result<T> visit(RwStreamSort sort);

  Result<T> visit(RwStreamHashJoin hashJoin);

  Result<T> visit(RwStreamAgg aggregate);

  Result<T> visit(RwStreamExchange exchange);

  Result<T> visit(RwStreamFilter filter);

  Result<T> visit(RwStreamProject project);

  Result<T> visit(RwStreamTableSource tableSource);

  /** Result to return one node and other info */
  class Result<T> {
    @Nonnull public final RisingWaveStreamingRel node;

    @Nonnull public final T info;

    public Result(@Nonnull RisingWaveStreamingRel node, @Nonnull T info) {
      this.node = node;
      this.info = info;
    }
  }
}
