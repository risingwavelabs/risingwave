package com.risingwave.planner.util;

public class PlannerTestCase {
  private final String name;
  private final String sql;
  private final String plan;
  private final String json;

  public PlannerTestCase(String name, String sql, String plan, String json) {
    this.name = name;
    this.sql = sql;
    this.plan = plan;
    this.json = json;
  }

  public String getSql() {
    return sql;
  }

  public String getPlan() {
    return plan;
  }

  public String getJson() {
    return json;
  }

  @Override
  public String toString() {
    return name;
  }
}
