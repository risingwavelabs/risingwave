package org.apache.calcite.jdbc;

import org.apache.calcite.schema.Schema;

public class CalciteSchemaWrapper extends SimpleCalciteSchema {
  public CalciteSchemaWrapper(Schema schema, String name) {
    super(null, schema, name);
  }
}
