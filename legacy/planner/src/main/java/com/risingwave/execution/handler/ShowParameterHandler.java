package com.risingwave.execution.handler;

import com.risingwave.common.datatype.RisingWaveTypeFactory;
import com.risingwave.execution.context.ExecutionContext;
import com.risingwave.execution.result.SimpleQueryResult;
import com.risingwave.pgwire.msg.StatementType;
import com.risingwave.pgwire.types.Values;
import com.risingwave.sql.node.SqlShowParameters;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;

/** Handle `SHOW A` */
public class ShowParameterHandler implements SqlHandler {
  @Override
  public SimpleQueryResult handle(SqlNode ast, ExecutionContext context) {
    var plan = (SqlShowParameters) ast;
    var config = context.getSessionConfiguration();
    var ret = config.getByString(plan.getName().toString()).toString();
    var factory = RisingWaveTypeFactory.INSTANCE;
    // Currently all result are return in string format.
    var type =
        factory.createStructType(
            List.of(factory.createSqlType(SqlTypeName.VARCHAR)), List.of("SHOW PARAMETERS"));
    var lines =
        Arrays.asList(transformToPg(ret)).stream()
            .map(Values::createString)
            .map(List::of)
            .collect(Collectors.toList());
    return new SimpleQueryResult(StatementType.SHOW_PARAMETERS, type, lines);
  }

  /**
   * Pre-process the return value to be consistent with PG. For example, enable_hashagg is on/off
   * instead of true/false. It is hard to be consistent if we store the boolean in configuration.
   * This workaround is ok for now.
   */
  private String transformToPg(String val) {
    if (val == "true") {
      return "on";
    }

    if (val == "false") {
      return "off";
    }

    return val;
  }
}
