package com.risingwave.planner.rel.serialization;

import java.io.PrintWriter;
import org.apache.calcite.rel.externalize.RelWriterImpl;
import org.apache.calcite.sql.SqlExplainLevel;

public class ExplainWriter extends RelWriterImpl {
  public ExplainWriter(PrintWriter printWriter) {
    super(printWriter, SqlExplainLevel.EXPPLAN_ATTRIBUTES, false);
  }
}
