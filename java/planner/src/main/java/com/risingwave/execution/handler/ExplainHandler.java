package com.risingwave.execution.handler;

import com.risingwave.common.datatype.RisingWaveTypeFactory;
import com.risingwave.execution.context.ExecutionContext;
import com.risingwave.execution.result.SimpleQueryResult;
import com.risingwave.pgwire.database.PgResult;
import com.risingwave.pgwire.msg.StatementType;
import com.risingwave.pgwire.types.Values;
import com.risingwave.planner.planner.batch.BatchPlanner;
import com.risingwave.planner.rel.physical.batch.BatchPlan;
import com.risingwave.planner.rel.serialization.ExplainWriter;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.sql.SqlExplain;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;

@HandlerSignature(sqlKinds = {SqlKind.EXPLAIN})
public class ExplainHandler implements SqlHandler {

  @Override
  public PgResult handle(SqlNode ast, ExecutionContext context) {
    final SqlNode query = ((SqlExplain) ast).getExplicandum();

    BatchPlanner planner = new BatchPlanner();
    BatchPlan plan = planner.plan(query, context);

    var explained = ExplainWriter.explainPlan(plan.getRoot());

    var factory = new RisingWaveTypeFactory();
    var type =
        factory.createStructType(
            List.of(factory.createSqlType(SqlTypeName.VARCHAR)), List.of("QUERY PLAN"));
    var lines =
        Arrays.asList(explained.split(System.lineSeparator())).stream()
            .map(Values::createString)
            .map(List::of)
            .collect(Collectors.toList());

    return new SimpleQueryResult(StatementType.EXPLAIN, type, lines);
  }
}
