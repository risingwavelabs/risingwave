package com.risingwave.planner.util;

import java.util.Optional;

/** Abstraction for a single test case. Load by TestCase Loader. */
public class PlannerTestCase {
  private final String name;
  private final String sql;
  // compatible with stream plan test
  @Deprecated private final Optional<String> plan;
  private final Optional<String> phyPlan;
  private final Optional<String> distPlan;
  private final Optional<String> json;

  public PlannerTestCase(
      String name, String sql, String plan, String phyPlan, String distPlan, String json) {
    this.name = name;
    this.sql = sql;
    this.plan = Optional.ofNullable(plan);
    this.phyPlan = Optional.ofNullable(phyPlan);
    this.distPlan = Optional.ofNullable(distPlan);
    this.json = Optional.ofNullable(json);
  }

  public String getSql() {
    return sql;
  }

  public Optional<String> getPlan() {
    return plan;
  }

  public Optional<String> getPhyPlan() {
    return phyPlan;
  }

  public Optional<String> getDistPlan() {
    return distPlan;
  }

  public Optional<String> getJson() {
    return json;
  }

  @Override
  public String toString() {
    return name;
  }
}
