package com.risingwave.planner.planner;

import com.risingwave.catalog.CatalogService;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.RelOptRuleCall;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Context for storing configurations of a planner. */
public class PlannerContext implements Context {
  private final CatalogService catalogService;

  public PlannerContext(CatalogService catalogService) {
    this.catalogService = catalogService;
  }

  public CatalogService getCatalogService() {
    return catalogService;
  }

  @Override
  public <C> @Nullable C unwrap(Class<C> klass) {
    if (klass.isInstance(this)) {
      return klass.cast(this);
    } else {
      return null;
    }
  }

  public static PlannerContext from(RelOptRuleCall call) {
    return call.getPlanner().getContext().unwrapOrThrow(PlannerContext.class);
  }
}
