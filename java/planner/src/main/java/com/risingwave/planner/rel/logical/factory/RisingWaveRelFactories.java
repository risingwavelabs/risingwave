package com.risingwave.planner.rel.logical.factory;

import com.risingwave.planner.rel.logical.RwLogicalFilter;
import com.risingwave.planner.rel.logical.RwLogicalJoin;
import com.risingwave.planner.rel.logical.RwLogicalProject;
import org.apache.calcite.rel.core.RelFactories;

public class RisingWaveRelFactories {
  public static final RelFactories.ProjectFactory PROJECT = RwLogicalProject::create;
  public static final RelFactories.FilterFactory FILTER = RwLogicalFilter::create;
  public static final RelFactories.JoinFactory JOIN = RwLogicalJoin::create;
}
