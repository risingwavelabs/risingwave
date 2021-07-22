package com.risingwave.planner.sql;

import com.risingwave.common.datatype.RisingWaveTypeFactory;
import java.util.List;
import java.util.Properties;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.SchemaPlus;
import org.checkerframework.checker.nullness.qual.Nullable;

public class RisingWaveCalciteCatalogReader extends CalciteCatalogReader
    implements RelOptTable.ViewExpander {
  public RisingWaveCalciteCatalogReader(
      SchemaPlus rootSchema, List<String> defaultSchema, RisingWaveTypeFactory typeFactory) {
    super(CalciteSchema.from(rootSchema), defaultSchema, typeFactory, getConnectionConfig());
  }

  private static CalciteConnectionConfigImpl getConnectionConfig() {
    Properties properties = new Properties();
    properties.setProperty(
        CalciteConnectionProperty.CASE_SENSITIVE.camelName(), String.valueOf(true));
    return new CalciteConnectionConfigImpl(properties);
  }

  @Override
  public RelRoot expandView(
      RelDataType rowType,
      String queryString,
      List<String> schemaPath,
      @Nullable List<String> viewPath) {
    throw new UnsupportedOperationException("expandView");
  }
}
