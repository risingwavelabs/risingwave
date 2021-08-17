package com.risingwave.sql;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;

import com.google.common.base.Verify;
import com.risingwave.common.exception.PgErrorCode;
import com.risingwave.common.exception.PgException;
import com.risingwave.planner.sql.RisingWaveOperatorTable;
import com.risingwave.sql.tree.AllColumns;
import com.risingwave.sql.tree.ArithmeticExpression;
import com.risingwave.sql.tree.AstVisitor;
import com.risingwave.sql.tree.Cast;
import com.risingwave.sql.tree.ColumnDefinition;
import com.risingwave.sql.tree.ColumnType;
import com.risingwave.sql.tree.ComparisonExpression;
import com.risingwave.sql.tree.CreateTable;
import com.risingwave.sql.tree.DoubleLiteral;
import com.risingwave.sql.tree.DropTable;
import com.risingwave.sql.tree.FunctionCall;
import com.risingwave.sql.tree.Insert;
import com.risingwave.sql.tree.IntegerLiteral;
import com.risingwave.sql.tree.LongLiteral;
import com.risingwave.sql.tree.Node;
import com.risingwave.sql.tree.NotNullColumnConstraint;
import com.risingwave.sql.tree.QualifiedName;
import com.risingwave.sql.tree.QualifiedNameReference;
import com.risingwave.sql.tree.Query;
import com.risingwave.sql.tree.QuerySpecification;
import com.risingwave.sql.tree.SingleColumn;
import com.risingwave.sql.tree.StringLiteral;
import com.risingwave.sql.tree.Table;
import com.risingwave.sql.tree.Values;
import com.risingwave.sql.tree.ValuesList;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlBasicTypeNameSpec;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.ddl.SqlDdlNodes;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlNameMatcher;
import org.apache.calcite.sql.validate.SqlNameMatchers;

public class ToCalciteAstVisitor extends AstVisitor<SqlNode, Void> {
  private final RisingWaveOperatorTable operatorTable = new RisingWaveOperatorTable();

  @Override
  public SqlNode visitCreateTable(CreateTable<?> node, Void context) {
    boolean ifNotExists = node.ifNotExists();
    SqlIdentifier name = visitTable(node.name(), context);
    SqlNodeList columnList =
        SqlNodeList.of(
            SqlParserPos.ZERO,
            node.tableElements().stream()
                .map(column -> column.accept(this, context))
                .collect(Collectors.toList()));

    return SqlDdlNodes.createTable(SqlParserPos.ZERO, false, ifNotExists, name, columnList, null);
  }

  @Override
  public SqlIdentifier visitTable(Table<?> table, Void context) {
    return new SqlIdentifier(table.getName().getParts(), SqlParserPos.ZERO);
  }

  @Override
  public SqlNode visitColumnDefinition(ColumnDefinition<?> columnDefinition, Void context) {
    SqlIdentifier name = new SqlIdentifier(columnDefinition.ident(), SqlParserPos.ZERO);
    SqlDataTypeSpec dataTypeSpec = visitColumnType(columnDefinition.type(), context);
    boolean notNull =
        columnDefinition.constraints().stream().anyMatch(c -> c instanceof NotNullColumnConstraint);

    ColumnStrategy columnStrategy;
    if (notNull) {
      dataTypeSpec = dataTypeSpec.withNullable(Boolean.FALSE);
      columnStrategy = ColumnStrategy.NOT_NULLABLE;
    } else {
      dataTypeSpec = dataTypeSpec.withNullable(Boolean.TRUE);
      columnStrategy = ColumnStrategy.NULLABLE;
    }

    return SqlDdlNodes.column(SqlParserPos.ZERO, name, dataTypeSpec, null, columnStrategy);
  }

  @Override
  public SqlDataTypeSpec visitColumnType(ColumnType<?> columnType, Void context) {
    return new SqlDataTypeSpec(toBasicTypeNameSpec(columnType), SqlParserPos.ZERO);
  }

  @Override
  public SqlNode visitQuery(Query node, Void context) {
    return node.getQueryBody().accept(this, context);
  }

  @Override
  public SqlNode visitAllColumns(AllColumns node, Void context) {
    if (node.getPrefix().isPresent()) {
      List<String> prefixes = node.getPrefix().get().getParts();
      List<SqlParserPos> poses =
          prefixes.stream().map(x -> SqlParserPos.ZERO).collect(Collectors.toList());
      return SqlIdentifier.star(prefixes, SqlParserPos.ZERO, poses);
    } else {
      return SqlIdentifier.star(SqlParserPos.ZERO);
    }
  }

  @Override
  public SqlNode visitQuerySpecification(QuerySpecification node, Void context) {
    checkArgument(!node.getHaving().isPresent(), "Having not supported yet!");
    checkArgument(node.getWindows().isEmpty(), "Window not supported yet!");
    checkArgument(node.getOrderBy().isEmpty(), "Order by not supported yet!");
    checkArgument(!node.getLimit().isPresent(), "Limit not supported yet!");
    checkArgument(!node.getOffset().isPresent(), "Offset not supported yet!");

    SqlNodeList selectList =
        SqlNodeList.of(
            SqlParserPos.ZERO,
            node.getSelect().getSelectItems().stream()
                .map(item -> item.accept(this, context))
                .collect(Collectors.toList()));

    verify(node.getFrom().size() <= 1, "Currently only support selecting from one table!");
    SqlNode from = null;
    if (node.getFrom().size() > 0) {
      from = node.getFrom().get(0).accept(this, context);
    }

    SqlNode where = node.getWhere().map(exp -> exp.accept(this, context)).orElse(null);
    SqlNodeList groupBy = null;
    if (!node.getGroupBy().isEmpty()) {
      groupBy =
          SqlNodeList.of(
              SqlParserPos.ZERO,
              node.getGroupBy().stream()
                  .map(exp -> exp.accept(this, context))
                  .collect(Collectors.toList()));
    }

    return new SqlSelect(
        SqlParserPos.ZERO,
        null,
        selectList,
        from,
        where,
        groupBy,
        null,
        null,
        null,
        null,
        null,
        null);
  }

  @Override
  public SqlNode visitDropTable(DropTable<?> node, Void context) {
    SqlIdentifier tableName = visitTable(node.table(), context);
    return SqlDdlNodes.dropTable(SqlParserPos.ZERO, false, tableName);
  }

  @Override
  protected SqlNode visitQualifiedNameReference(QualifiedNameReference node, Void context) {
    return new SqlIdentifier(node.getName().getParts(), SqlParserPos.ZERO);
  }

  @Override
  protected SqlNode visitArithmeticExpression(ArithmeticExpression node, Void context) {
    SqlNode left = node.getLeft().accept(this, context);
    SqlNode right = node.getRight().accept(this, context);

    SqlOperator operator;
    switch (node.getType()) {
      case ADD:
        operator = SqlStdOperatorTable.PLUS;
        break;
      case SUBTRACT:
        operator = SqlStdOperatorTable.MINUS;
        break;
      case MULTIPLY:
        operator = SqlStdOperatorTable.MULTIPLY;
        break;
      case DIVIDE:
        operator = SqlStdOperatorTable.DIVIDE;
        break;
      case MODULUS:
        operator = SqlStdOperatorTable.MOD;
        break;
      default:
        throw new PgException(
            PgErrorCode.SYNTAX_ERROR, "Unknown arithmetic operator: %s", node.getType().getValue());
    }

    return new SqlBasicCall(operator, new SqlNode[] {left, right}, SqlParserPos.ZERO);
  }

  @Override
  protected SqlNode visitNode(Node node, Void context) {
    throw new UnsupportedOperationException("Unknown node: " + node);
  }

  @Override
  public SqlNode visitInsert(Insert<?> node, Void context) {
    SqlNodeList keywords = SqlNodeList.EMPTY;
    SqlNode table = visitTable(node.table(), context);
    SqlNode source = visitQuery(node.insertSource(), context);
    SqlNodeList columnList = null;
    if (!node.columns().isEmpty()) {
      columnList =
          SqlNodeList.of(
              SqlParserPos.ZERO,
              node.columns().stream()
                  .map(c -> new SqlIdentifier(Collections.singletonList(c), SqlParserPos.ZERO))
                  .collect(Collectors.toList()));
    }

    return new SqlInsert(SqlParserPos.ZERO, keywords, table, source, columnList);
  }

  @Override
  protected SqlNode visitFunctionCall(FunctionCall node, Void context) {
    checkArgument(!node.isDistinct(), "Distinct not supported yet!");
    checkArgument(!node.filter().isPresent(), "Filter in function call not supported!");
    checkArgument(!node.getWindow().isPresent(), "Window in function call not supported!");

    SqlIdentifier functionName = identifierOf(node.getName());
    SqlOperator operator = lookupOperator(functionName, SqlSyntax.FUNCTION);

    SqlNode[] operands =
        node.getArguments().stream().map(exp -> exp.accept(this, context)).toArray(SqlNode[]::new);
    return new SqlBasicCall(operator, operands, SqlParserPos.ZERO);
  }

  @Override
  public SqlNode visitValues(Values values, Void context) {
    SqlNode[] operands =
        values.rows().stream().map(row -> row.accept(this, context)).toArray(SqlNode[]::new);

    return new SqlBasicCall(SqlStdOperatorTable.VALUES, operands, SqlParserPos.ZERO);
  }

  @Override
  protected SqlNode visitIntegerLiteral(IntegerLiteral node, Void context) {
    return SqlLiteral.createExactNumeric(String.valueOf(node.getValue()), SqlParserPos.ZERO);
  }

  @Override
  protected SqlNode visitLongLiteral(LongLiteral node, Void context) {
    return SqlLiteral.createExactNumeric(String.valueOf(node.getValue()), SqlParserPos.ZERO);
  }

  @Override
  protected SqlNode visitDoubleLiteral(DoubleLiteral node, Void context) {
    // TODO: Optimize this
    String value = BigDecimal.valueOf(node.getValue()).toString();
    return SqlLiteral.createApproxNumeric(value, SqlParserPos.ZERO);
  }

  @Override
  protected SqlNode visitStringLiteral(StringLiteral node, Void context) {
    return SqlLiteral.createCharString(node.getValue(), SqlParserPos.ZERO);
  }

  @Override
  public SqlNode visitValuesList(ValuesList node, Void context) {
    SqlNode[] operands =
        node.values().stream().map(expr -> expr.accept(this, context)).toArray(SqlNode[]::new);

    return new SqlBasicCall(SqlStdOperatorTable.ROW, operands, SqlParserPos.ZERO);
  }

  @Override
  protected SqlNode visitComparisonExpression(ComparisonExpression node, Void context) {
    SqlIdentifier functionName = new SqlIdentifier(node.getType().getValue(), SqlParserPos.ZERO);
    SqlOperator operator = lookupOperator(functionName, SqlSyntax.BINARY);

    SqlNode left = node.getLeft().accept(this, context);
    SqlNode right = node.getRight().accept(this, context);
    return new SqlBasicCall(operator, new SqlNode[] {left, right}, SqlParserPos.ZERO);
  }

  @Override
  protected SqlNode visitSingleColumn(SingleColumn node, Void context) {
    checkArgument(node.getAlias() == null, "Alias not supported yet!");
    return node.getExpression().accept(this, context);
  }

  @Override
  protected SqlNode visitCast(Cast node, Void context) {
    SqlOperator operator = SqlStdOperatorTable.CAST;

    SqlNode operand = node.getExpression().accept(this, context);
    SqlNode targetType =
        new SqlDataTypeSpec(toBasicTypeNameSpec(node.getType()), SqlParserPos.ZERO);

    return new SqlBasicCall(operator, new SqlNode[] {operand, targetType}, SqlParserPos.ZERO);
  }

  private static SqlBasicTypeNameSpec toBasicTypeNameSpec(ColumnType<?> columnType) {
    String typeName = columnType.name().toUpperCase();
    switch (typeName) {
      case "SMALLINT":
        return new SqlBasicTypeNameSpec(SqlTypeName.SMALLINT, SqlParserPos.ZERO);
      case "INT":
      case "INTEGER":
        return new SqlBasicTypeNameSpec(SqlTypeName.INTEGER, SqlParserPos.ZERO);
      case "BIGINT":
        return new SqlBasicTypeNameSpec(SqlTypeName.BIGINT, SqlParserPos.ZERO);
      case "FLOAT":
      case "REAL":
        return new SqlBasicTypeNameSpec(SqlTypeName.FLOAT, SqlParserPos.ZERO);
      case "DOUBLE":
      case "DOUBLE PRECISION":
        return new SqlBasicTypeNameSpec(SqlTypeName.DOUBLE, SqlParserPos.ZERO);
      case "DATE":
        return new SqlBasicTypeNameSpec(SqlTypeName.DATE, SqlParserPos.ZERO);
      case "TIME":
        return new SqlBasicTypeNameSpec(SqlTypeName.TIME, SqlParserPos.ZERO);
      case "TIMESTAMP":
        return new SqlBasicTypeNameSpec(SqlTypeName.TIMESTAMP, SqlParserPos.ZERO);
      case "CHAR":
        {
          var parameters = columnType.parameters();
          Verify.verify(parameters.size() <= 1, "The parameter list of CHAR is too long");
          if (parameters.size() == 0) {
            // If user do not specify length, the default limit is 1.
            return new SqlBasicTypeNameSpec(SqlTypeName.CHAR, 1, SqlParserPos.ZERO);
          } else {
            return new SqlBasicTypeNameSpec(SqlTypeName.CHAR, parameters.get(0), SqlParserPos.ZERO);
          }
        }
      case "VARCHAR":
        {
          var parameters = columnType.parameters();
          Verify.verify(parameters.size() <= 1, "The parameter list of VARCHAR is too long");
          if (parameters.size() == 0) {
            // If user do not specify length, there is no limit. Use -1 here.
            return new SqlBasicTypeNameSpec(SqlTypeName.VARCHAR, -1, SqlParserPos.ZERO);
          } else {
            return new SqlBasicTypeNameSpec(
                SqlTypeName.VARCHAR, parameters.get(0), SqlParserPos.ZERO);
          }
        }
      case "NUMERIC":
        {
          var parameters = columnType.parameters();
          if (parameters.size() == 0) {
            return new SqlBasicTypeNameSpec(SqlTypeName.DECIMAL, SqlParserPos.ZERO);
          } else if (parameters.size() == 1) {
            return new SqlBasicTypeNameSpec(
                SqlTypeName.DECIMAL, parameters.get(0), SqlParserPos.ZERO);
          } else {
            return new SqlBasicTypeNameSpec(
                SqlTypeName.DECIMAL, parameters.get(0), parameters.get(1), SqlParserPos.ZERO);
          }
        }

      default:
        throw new PgException(PgErrorCode.SYNTAX_ERROR, "Unsupported type name: %s", typeName);
    }
  }

  private static SqlIdentifier identifierOf(QualifiedName name) {
    return new SqlIdentifier(name.getParts(), SqlParserPos.ZERO);
  }

  private SqlOperator lookupOperator(SqlIdentifier functionName, SqlSyntax syntax) {
    List<SqlOperator> result = new ArrayList<>();
    SqlNameMatcher nameMatcher = SqlNameMatchers.withCaseSensitive(false);

    operatorTable.lookupOperatorOverloads(functionName, null, syntax, result, nameMatcher);
    if (result.size() < 1) {
      throw new PgException(PgErrorCode.SYNTAX_ERROR, "Function not found: %s", functionName);
    } else if (result.size() > 1) {
      throw new PgException(
          PgErrorCode.SYNTAX_ERROR, "Too many function not found: %s", functionName);
    }

    return result.get(0);
  }

  // Don't remove this, it's useful for debugging.
  //  public static void main(String[] args) throws SqlParseException {
  //    SqlParser.Config parserConfig = SqlParser.Config.DEFAULT
  //        .withCaseSensitive(false)
  //        .withLex(Lex.SQL_SERVER);
  //
  //    String sql = "SELECT CAST(25.65 AS int)";
  ////    String sql = "select 100.0::DOUBLE/8.0::DOUBLE";
  //    SqlNode sqlNode = SqlParser.create(sql, parserConfig)
  //        .parseQuery();
  //    System.out.println(sqlNode.toString());
  //  }
}
