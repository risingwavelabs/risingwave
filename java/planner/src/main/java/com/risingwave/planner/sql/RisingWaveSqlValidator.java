package com.risingwave.planner.sql;

import org.apache.calcite.config.NullCollation;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.validate.SqlValidatorCatalogReader;
import org.apache.calcite.sql.validate.SqlValidatorImpl;

public class RisingWaveSqlValidator extends SqlValidatorImpl {
  private static final Config DEFAULT_CONFIG =
      Config.DEFAULT
          .withCallRewrite(false)
          .withDefaultNullCollation(NullCollation.HIGH)
          .withColumnReferenceExpansion(true)
          .withSqlConformance(new RisingWaveConformance())
          .withIdentifierExpansion(true);

  public RisingWaveSqlValidator(
      SqlOperatorTable opTab,
      SqlValidatorCatalogReader catalogReader,
      RelDataTypeFactory typeFactory) {
    super(opTab, catalogReader, typeFactory, DEFAULT_CONFIG);
  }
}
