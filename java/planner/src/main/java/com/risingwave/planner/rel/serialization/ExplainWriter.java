package com.risingwave.planner.rel.serialization;

import java.io.PrintWriter;
import java.io.StringWriter;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.externalize.RelWriterImpl;
import org.apache.calcite.sql.SqlExplainLevel;

public class ExplainWriter extends RelWriterImpl {
  public ExplainWriter(PrintWriter printWriter) {
    super(printWriter, SqlExplainLevel.EXPPLAN_ATTRIBUTES, false);
  }

  public static String explainToString(RelNode relNode) {
    StringWriter sw = new StringWriter();
    PrintWriter printer = new PrintWriter(sw);
    ExplainWriter writer = new ExplainWriter(printer);
    relNode.explain(writer);
    return sw.toString().trim();
  }
}
