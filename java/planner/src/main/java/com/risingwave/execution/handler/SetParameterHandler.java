package com.risingwave.execution.handler;

import com.risingwave.execution.context.ExecutionContext;
import com.risingwave.execution.result.SetParameterResult;
import com.risingwave.pgwire.msg.StatementType;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSetOption;

/** Handle `SET A to B` */
@HandlerSignature(sqlKinds = {SqlKind.SET_OPTION})
public class SetParameterHandler implements SqlHandler {
  @Override
  public SetParameterResult handle(SqlNode ast, ExecutionContext context) {
    var setStat = (SqlSetOption) ast;

    // Store the config param into context.
    var config = context.getConf();
    // Filter out the annoying "''" in configuration key.
    config.setByString(
        setStat.getName().getSimple(), setStat.getValue().toString().replace("'", ""));
    return new SetParameterResult(StatementType.SET_OPTION);
  }
}
