package com.risingwave.sql;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterators.singletonIterator;
import static com.google.common.collect.Iterators.toArray;
import static com.risingwave.sql.AstUtils.identifierOf;
import static com.risingwave.sql.AstUtils.sqlNodeListOf;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;

import com.google.common.base.Verify;
import com.google.common.collect.Iterators;
import com.risingwave.common.collections.Lists2;
import com.risingwave.common.datatype.StringType;
import com.risingwave.common.exception.PgErrorCode;
import com.risingwave.common.exception.PgException;
import com.risingwave.planner.sql.RisingWaveOperatorTable;
import com.risingwave.sql.node.*;
import com.risingwave.sql.tree.*;
import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.ddl.SqlDdlNodes;
import org.apache.calcite.sql.fun.SqlCase;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;
import org.checkerframework.checker.nullness.qual.Nullable;

/** A visitor for transforming sqlNode to calcite node */
public class ToCalciteAstVisitor extends AstVisitor<SqlNode, Void> {
  private final RisingWaveOperatorTable operatorTable = new RisingWaveOperatorTable();
  final int intervalPrecision = 9;

  @Override
  public SqlNode visitCreateTable(CreateTable<?> node, Void context) {
    boolean ifNotExists = node.ifNotExists();
    SqlIdentifier name = visitTable(node.name(), context);
    SqlNodeList columnList =
        sqlNodeListOf(Lists2.map(node.tableElements(), column -> column.accept(this, context)));

    return SqlDdlNodes.createTable(SqlParserPos.ZERO, false, ifNotExists, name, columnList, null);
  }

  @Override
  public SqlNode visitFlush(Flush<?> node, Void context) {
    return new SqlFlush(SqlParserPos.ZERO);
  }

  @Override
  public SqlNode visitCreateView(CreateView createView, Void context) {
    SqlIdentifier name = new SqlIdentifier(createView.name().getParts(), SqlParserPos.ZERO);
    Query query = createView.query();
    checkArgument(
        query.getQueryBody() instanceof QuerySpecification, "Must create view from a query");
    QuerySpecification specification = ((QuerySpecification) query.getQueryBody());

    SqlNode queryNode = visitQuery(query, context);

    SqlNodeList selectList =
        SqlNodeList.of(
            SqlParserPos.ZERO,
            specification.getSelect().getSelectItems().stream()
                .map(item -> item.accept(this, context))
                .collect(Collectors.toList()));

    if (createView.isMaterialized()) {
      return SqlDdlNodes.createMaterializedView(
          SqlParserPos.ZERO, false, false, name, selectList, queryNode);
    } else {
      return SqlDdlNodes.createView(SqlParserPos.ZERO, false, name, selectList, queryNode);
    }
  }

  @Override
  public SqlNode visitCreateSource(CreateSource node, Void context) {
    var pos = SqlParserPos.ZERO;

    SqlIdentifier name = new SqlIdentifier(node.getName(), pos);
    SqlNodeList columnList =
        sqlNodeListOf(
            node.getTableElements().stream()
                .map(column -> column.accept(this, context))
                .collect(Collectors.toList()));

    // TODO(peng): dirty hack for primary keys
    List<ColumnDefinition<?>> primaryColumns =
        node.getTableElements().stream()
            .filter(
                e ->
                    (e instanceof ColumnDefinition)
                        && ((ColumnDefinition<?>) e)
                            .constraints().stream()
                                .anyMatch(c -> c instanceof PrimaryKeyColumnConstraint))
            .map(e -> ((ColumnDefinition<?>) e))
            .collect(Collectors.toList());

    SqlNodeList properties = visitGenericProperties(node.getProperties(), context);
    SqlCharStringLiteral rowFormat = SqlLiteral.createCharString(node.getRowFormat(), pos);
    SqlCharStringLiteral rowSchemaLocation =
        SqlLiteral.createCharString(node.getRowSchemaLocation(), pos);
    return new SqlCreateSource(
        pos, name, columnList, properties, rowFormat, rowSchemaLocation, primaryColumns);
  }

  @Override
  public SqlNode visitDropView(DropView dropView, Void context) {
    SqlIdentifier name = new SqlIdentifier(dropView.name().getParts(), SqlParserPos.ZERO);

    if (dropView.isMaterialized()) {
      return SqlDdlNodes.dropMaterializedView(SqlParserPos.ZERO, dropView.ifExists(), name);
    } else {
      return SqlDdlNodes.dropView(SqlParserPos.ZERO, dropView.ifExists(), name);
    }
  }

  @Override
  public SqlIdentifier visitTable(Table<?> table, Void context) {
    return new SqlIdentifier(table.getName().getParts(), SqlParserPos.ZERO);
  }

  @Override
  public SqlNodeList visitGenericProperties(GenericProperties<?> node, Void context) {
    var pos = SqlParserPos.ZERO;
    return sqlNodeListOf(
        Lists2.map(
            node.properties().entrySet(),
            prop -> {
              var name = SqlLiteral.createCharString(prop.getKey(), pos);
              var value = ((Expression) prop.getValue()).accept(this, context);
              return new SqlTableOption(name, value, pos);
            }));
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
      prefixes.add("*");
      List<SqlParserPos> poses =
          prefixes.stream().map(x -> SqlParserPos.ZERO).collect(Collectors.toList());
      return SqlIdentifier.star(prefixes, SqlParserPos.ZERO, poses);
    } else {
      return SqlIdentifier.star(SqlParserPos.ZERO);
    }
  }

  @Override
  public SqlNode visitQuerySpecification(QuerySpecification node, Void context) {
    checkArgument(node.getWindows().isEmpty(), "Window not supported yet!");

    SqlNodeList selectList =
        sqlNodeListOf(
            node.getSelect().getSelectItems().stream()
                .map(item -> item.accept(this, context))
                .collect(Collectors.toList()));

    var fromNodes =
        node.getFrom().stream().map(f -> f.accept(this, context)).collect(Collectors.toList());

    var from = fromNodes.stream().reduce(ToCalciteAstVisitor::toCommaJoin).orElse(null);

    SqlNode where = node.getWhere().map(exp -> exp.accept(this, context)).orElse(null);
    SqlNodeList groupBy = null;
    if (!node.getGroupBy().isEmpty()) {
      groupBy =
          sqlNodeListOf(
              node.getGroupBy().stream()
                  .map(exp -> exp.accept(this, context))
                  .collect(Collectors.toList()));
    }

    var having = node.getHaving().map(exp -> exp.accept(this, context)).orElse(null);
    var offset = node.getOffset().map(off -> off.accept(this, context)).orElse(null);
    var limit = node.getLimit().map(lim -> lim.accept(this, context)).orElse(null);
    var selectNode =
        new SqlSelect(
            SqlParserPos.ZERO,
            null,
            selectList,
            from,
            where,
            groupBy,
            having,
            null,
            null,
            offset,
            limit,
            null);

    SqlNode ret = selectNode;
    if (!node.getOrderBy().isEmpty()) {
      // When it's SqlOrderBy node, we should select all tuples and sort.
      selectNode.setFetch(null);
      selectNode.setOffset(null);
      var orderList =
          sqlNodeListOf(
              node.getOrderBy().stream()
                  .map(orderByItem -> orderByItem.accept(this, context))
                  .collect(Collectors.toList()));
      ret = new SqlOrderBy(SqlParserPos.ZERO, selectNode, orderList, offset, limit);
    }
    return ret;
  }

  private static SqlJoin toCommaJoin(SqlNode left, SqlNode right) {
    requireNonNull(left, "left");
    requireNonNull(right, "right");

    return new SqlJoin(
        SqlParserPos.ZERO,
        left,
        SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
        JoinType.COMMA.symbol(SqlParserPos.ZERO),
        right,
        JoinConditionType.NONE.symbol(SqlParserPos.ZERO),
        null);
  }

  @Override
  protected SqlNode visitSortItem(SortItem node, Void context) {
    checkArgument(
        node.getNullOrdering() == SortItem.NullOrdering.UNDEFINED,
        "Null ordering not supported now!");

    var sortKey = node.getSortKey().accept(this, context);

    var ret = sortKey;
    // If ordering is null or asc, it's treated asc in calcite.
    if (node.getOrdering() == SortItem.Ordering.DESCENDING) {
      var operator = operatorTable.lookupOneOperator(identifierOf("DESC"), SqlSyntax.POSTFIX);
      ret = new SqlBasicCall(operator, new SqlNode[] {sortKey}, SqlParserPos.ZERO);
    }

    return ret;
  }

  @Override
  public SqlNode visitDropTable(DropTable<?> node, Void context) {
    SqlIdentifier tableName = visitTable(node.table(), context);
    return SqlDdlNodes.dropTable(SqlParserPos.ZERO, node.dropIfExists(), tableName);
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
  protected SqlNode visitExplain(Explain node, Void context) {
    SqlNode statement = visitStatement(node.getStatement(), context);

    // Currently, we only support default behavior of EXPLAIN as below
    final SqlExplainLevel detailLevel = SqlExplainLevel.EXPPLAN_ATTRIBUTES;
    final SqlExplain.Depth depth = SqlExplain.Depth.PHYSICAL;
    final SqlExplainFormat format = SqlExplainFormat.TEXT;

    return new SqlExplain(
        SqlParserPos.ZERO,
        statement,
        detailLevel.symbol(SqlParserPos.ZERO),
        depth.symbol(SqlParserPos.ZERO),
        format.symbol(SqlParserPos.ZERO),
        0);
  }

  @Override
  protected SqlNode visitStatement(Statement node, Void context) {
    return node.accept(this, context);
  }

  @Override
  public SqlNode visitInsert(Insert<?> node, Void context) {
    SqlNodeList keywords = SqlNodeList.EMPTY;
    SqlNode table = visitTable(node.table(), context);
    SqlNode source = visitQuery(node.insertSource(), context);
    SqlNodeList columnList = null;
    if (!node.columns().isEmpty()) {
      columnList =
          sqlNodeListOf(
              node.columns().stream()
                  .map(c -> new SqlIdentifier(singletonList(c), SqlParserPos.ZERO))
                  .collect(Collectors.toList()));
    }

    return new SqlInsert(SqlParserPos.ZERO, keywords, table, source, columnList);
  }

  @Override
  public SqlNode visitDelete(Delete node, Void context) {
    // TODO: suppport alias and returning
    var table = visitTable((Table<?>) node.getRelation(), context);
    var where = node.getWhere().map(exp -> exp.accept(this, context)).orElse(null);

    return new SqlDelete(SqlParserPos.ZERO, table, where, null, null);
  }

  @Override
  public SqlNode visitTableFunction(TableFunction node, Void context) {
    return visitFunctionCall(node.functionCall(), context);
  }

  @Override
  protected SqlNode visitFunctionCall(FunctionCall node, Void context) {
    checkArgument(node.filter().isEmpty(), "Filter in function call not supported!");
    checkArgument(node.getWindow().isEmpty(), "Window in function call not supported!");

    var functionNameStr = node.getName();
    if (functionNameStr.toString().equals("substr")) {
      // Be compatible with calcite
      functionNameStr = new QualifiedName("SUBSTRING");
    }

    SqlIdentifier functionName = identifierOf(functionNameStr);
    SqlOperator operator = lookupOperator(functionName, SqlSyntax.FUNCTION);

    SqlNode[] operands =
        node.getArguments().stream().map(exp -> exp.accept(this, context)).toArray(SqlNode[]::new);

    // Cases for count(*)
    if (operator.getKind() == SqlKind.COUNT && operands.length == 0) {
      operands = new SqlNode[] {SqlIdentifier.star(SqlParserPos.ZERO)};
    }

    SqlLiteral quantifier = null;
    if (node.isDistinct()) {
      quantifier = SqlSelectKeyword.DISTINCT.symbol(SqlParserPos.ZERO);
    }
    return new SqlBasicCall(operator, operands, SqlParserPos.ZERO, false, quantifier);
  }

  @Override
  public SqlNode visitValues(Values values, Void context) {
    SqlNode[] operands =
        values.rows().stream().map(row -> row.accept(this, context)).toArray(SqlNode[]::new);

    return new SqlBasicCall(SqlStdOperatorTable.VALUES, operands, SqlParserPos.ZERO);
  }

  @Override
  public SqlNode visitNullLiteral(NullLiteral node, Void context) {
    return SqlLiteral.createNull(SqlParserPos.ZERO);
  }

  @Override
  public SqlNode visitNegativeExpression(NegativeExpression node, Void context) {
    return SqlLiteral.createNegative(
        SqlLiteral.createExactNumeric(String.valueOf(node.getValue()), SqlParserPos.ZERO),
        SqlParserPos.ZERO);
  }

  @Override
  protected SqlNode visitIntegerLiteral(IntegerLiteral node, Void context) {
    return SqlLiteral.createExactNumeric(String.valueOf(node.getValue()), SqlParserPos.ZERO);
  }

  @Override
  protected SqlNode visitBooleanLiteral(BooleanLiteral node, Void context) {
    return SqlLiteral.createBoolean(node.getValue(), SqlParserPos.ZERO);
  }

  @Override
  protected SqlNode visitLongLiteral(LongLiteral node, Void context) {
    return SqlLiteral.createExactNumeric(String.valueOf(node.getValue()), SqlParserPos.ZERO);
  }

  @Override
  protected SqlNode visitDoubleLiteral(DoubleLiteral node, Void context) {
    // TODO: Optimize this
    BigDecimal decimal = new BigDecimal(node.getValueString());
    String value = decimal.toString();
    return SqlLiteral.createExactNumeric(value, SqlParserPos.ZERO);
  }

  @Override
  protected SqlNode visitStringLiteral(StringLiteral node, Void context) {
    return SqlLiteral.createCharString(node.getValue(), SqlParserPos.ZERO);
  }

  @Override
  public SqlNode visitIntervalLiteral(IntervalLiteral node, Void context) {
    checkArgument(node.getEndField() == null, "Doesn't support end field now!");
    int sign;
    switch (node.getSign()) {
      case PLUS:
        sign = 1;
        break;
      case MINUS:
        sign = -1;
        break;
      default:
        throw new PgException(
            PgErrorCode.INTERNAL_ERROR, "Unsupported type sign: %s", node.getSign());
    }

    var startUnit = TimeUnit.valueOf(node.getStartField().name());
    var endUnit =
        Optional.ofNullable(node.getEndField())
            .map(IntervalLiteral.IntervalField::name)
            .map(TimeUnit::valueOf)
            .orElse(null);
    // In TypeSystem, the max precision is 9. And the backend use int32 to store year/month.
    // Therefore, we hard code the precision 9 here.
    var intervalQualifier =
        new SqlIntervalQualifier(
            startUnit, intervalPrecision, endUnit, intervalPrecision, SqlParserPos.ZERO);
    return SqlLiteral.createInterval(sign, node.getValue(), intervalQualifier, SqlParserPos.ZERO);
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
    SqlNode sqlNode = node.getExpression().accept(this, context);
    if (node.getAlias() != null) {
      SqlOperator asCall = lookupOperator(identifierOf("AS"), SqlSyntax.SPECIAL);
      SqlNode[] operands = new SqlNode[] {sqlNode, identifierOf(node.getAlias())};
      sqlNode = new SqlBasicCall(asCall, operands, SqlParserPos.ZERO);
    }

    return sqlNode;
  }

  @Override
  protected SqlNode visitCast(Cast node, Void context) {
    SqlOperator operator = SqlStdOperatorTable.CAST;

    SqlNode operand = node.getExpression().accept(this, context);
    SqlNode targetType =
        new SqlDataTypeSpec(toBasicTypeNameSpec(node.getType()), SqlParserPos.ZERO);

    return new SqlBasicCall(operator, new SqlNode[] {operand, targetType}, SqlParserPos.ZERO);
  }

  @Override
  protected SqlNode visitLogicalBinaryExpression(LogicalBinaryExpression node, Void context) {
    SqlIdentifier operatorIdentifier;
    switch (node.getType()) {
      case AND:
        operatorIdentifier = identifierOf("AND");
        break;
      case OR:
        operatorIdentifier = identifierOf("OR");
        break;
      default:
        throw new PgException(
            PgErrorCode.INTERNAL_ERROR, "Unsupported logical expression type: %s", node.getType());
    }

    SqlOperator operator = lookupOperator(operatorIdentifier, SqlSyntax.BINARY);
    SqlNode[] operands =
        new SqlNode[] {node.getLeft().accept(this, context), node.getRight().accept(this, context)};

    return new SqlBasicCall(operator, operands, SqlParserPos.ZERO);
  }

  @Override
  protected SqlNode visitBetweenPredicate(BetweenPredicate node, Void context) {
    System.out.println(SqlStdOperatorTable.BETWEEN.getNameAsId());
    var operator = lookupOperator(identifierOf("BETWEEN SYMMETRIC"), SqlSyntax.SPECIAL);
    var operands =
        new SqlNode[] {
          node.getValue().accept(this, context),
          node.getMin().accept(this, context),
          node.getMax().accept(this, context)
        };

    return new SqlBasicCall(operator, operands, SqlParserPos.ZERO);
  }

  @Override
  protected SqlNode visitJoin(Join node, Void context) {
    var left = node.getLeft().accept(this, context);
    var natural =
        node.getCriteria()
            .map(c -> c instanceof NaturalJoin)
            .map(v -> SqlLiteral.createBoolean(v, SqlParserPos.ZERO))
            .orElse(SqlLiteral.createBoolean(false, SqlParserPos.ZERO));

    var right = node.getRight().accept(this, context);
    var joinType = JoinType.CROSS;
    switch (node.getType()) {
      case CROSS:
        joinType = JoinType.CROSS;
        break;
      case INNER:
        joinType = JoinType.INNER;
        break;
      case LEFT:
        joinType = JoinType.LEFT;
        break;
      case RIGHT:
        joinType = JoinType.RIGHT;
        break;
      case FULL:
        joinType = JoinType.FULL;
        break;
      default:
        throw new PgException(
            PgErrorCode.INTERNAL_ERROR, "Unrecognized join type: %s", node.getType());
    }

    SqlLiteral conditionType = null;
    SqlNode joinCondition = null;

    if (node.getCriteria().isPresent()) {
      var ret = joinConditionOf(node.getCriteria().get(), context);
      joinCondition = ret.left;
      conditionType = ret.right.symbol(SqlParserPos.ZERO);
    }

    return new SqlJoin(
        SqlParserPos.ZERO,
        left,
        natural,
        joinType.symbol(SqlParserPos.ZERO),
        right,
        conditionType,
        joinCondition);
  }

  private Pair<@Nullable SqlNode, JoinConditionType> joinConditionOf(
      JoinCriteria condition, Void context) {
    if (condition instanceof JoinUsing) {
      var columns =
          ((JoinUsing) condition)
              .getColumns().stream().map(AstUtils::identifierOf).collect(Collectors.toList());
      return Pair.of(new SqlNodeList(columns, SqlParserPos.ZERO), JoinConditionType.USING);
    } else if (condition instanceof JoinOn) {
      return Pair.of(
          ((JoinOn) condition).getExpression().accept(this, context), JoinConditionType.ON);
    } else if (condition instanceof NaturalJoin) {
      return Pair.of(null, JoinConditionType.NONE);
    } else {
      throw new UnsupportedOperationException("Unknown join criteria: " + condition);
    }
  }

  @Override
  protected SqlBasicCall visitLikePredicate(LikePredicate node, Void context) {
    checkArgument(node.getEscape() == null, "Escape not supported now!");
    checkArgument(!node.ignoreCase(), "Ignore not supported now");
    var operator = lookupOperator("LIKE", SqlSyntax.SPECIAL);
    var left = node.getValue().accept(this, context);
    var pattern = node.getPattern().accept(this, context);

    return new SqlBasicCall(operator, new SqlNode[] {left, pattern}, SqlParserPos.ZERO);
  }

  @Override
  protected SqlNode visitSubqueryExpression(SubqueryExpression node, Void context) {
    return node.getQuery().accept(this, context);
  }

  @Override
  protected SqlNode visitExists(ExistsPredicate node, Void context) {
    var existsCall = lookupOperator("EXISTS", SqlSyntax.PREFIX);
    var query = node.getSubquery().accept(this, context);
    return new SqlBasicCall(existsCall, new SqlNode[] {query}, SqlParserPos.ZERO);
  }

  @Override
  protected SqlNode visitAliasedRelation(AliasedRelation node, Void context) {
    var asCall = lookupOperator("AS", SqlSyntax.SPECIAL);
    var query = node.getRelation().accept(this, context);
    var alias = identifierOf(node.getAlias());
    var aliasColumns =
        node.getColumnNames().stream().map(AstUtils::identifierOf).collect(Collectors.toList());

    var operands =
        toArray(
            Iterators.concat(
                singletonIterator(query), singletonIterator(alias), aliasColumns.iterator()),
            SqlNode.class);
    return new SqlBasicCall(asCall, operands, SqlParserPos.ZERO);
  }

  @Override
  protected SqlNode visitTableSubquery(TableSubquery node, Void context) {
    return node.getQuery().accept(this, context);
  }

  @Override
  protected SqlNode visitExtract(Extract node, Void context) {
    var function = lookupOperator("EXTRACT", SqlSyntax.FUNCTION);
    var qualifier =
        new SqlIntervalQualifier(TimeUnit.valueOf(node.getField().name()), null, SqlParserPos.ZERO);
    var expression = node.getExpression().accept(this, context);
    return new SqlBasicCall(function, new SqlNode[] {qualifier, expression}, SqlParserPos.ZERO);
  }

  @Override
  protected SqlNode visitSearchedCaseExpression(SearchedCaseExpression node, Void context) {
    var whens =
        node.getWhenClauses().stream()
            .map(WhenClause::getOperand)
            .map(exp -> exp.accept(this, context))
            .collect(Collectors.toList());
    var thens =
        node.getWhenClauses().stream()
            .map(WhenClause::getResult)
            .map(exp -> exp.accept(this, context))
            .collect(Collectors.toList());

    var sqlNodeDefaultValue = node.getDefaultValue();
    SqlNode elseValue = null;
    if (sqlNodeDefaultValue != null) {
      elseValue = sqlNodeDefaultValue.accept(this, context);
    }

    return SqlCase.createSwitched(
        SqlParserPos.ZERO,
        null,
        new SqlNodeList(whens, SqlParserPos.ZERO),
        new SqlNodeList(thens, SqlParserPos.ZERO),
        elseValue);
  }

  @Override
  protected SqlBasicCall visitInPredicate(InPredicate node, Void context) {
    var inOperator = lookupOperator("IN", SqlSyntax.BINARY);
    var left = node.getValue().accept(this, context);
    var right = node.getValueList().accept(this, context);
    return new SqlBasicCall(inOperator, new SqlNode[] {left, right}, SqlParserPos.ZERO);
  }

  @Override
  protected SqlNode visitInListExpression(InListExpression node, Void context) {
    var values =
        node.getValues().stream().map(v -> v.accept(this, context)).collect(Collectors.toList());
    return new SqlNodeList(values, SqlParserPos.ZERO);
  }

  @Override
  public SqlNode visitSetStatement(SetStatement<?> node, Void context) {
    var assignment = node.assignments().get(0);
    if (node.scope() == node.scope().SESSION) {
      return new SqlSetOption(
          SqlParserPos.ZERO,
          null,
          new SqlIdentifier(
              ((QualifiedNameReference) assignment.columnName()).getName().toString(),
              SqlParserPos.ZERO),
          ((Expression) assignment.expression()).accept(this, context));
    } else {
      return new SqlSetOption(
          SqlParserPos.ZERO,
          "SYSTEM",
          new SqlIdentifier(
              ((QualifiedNameReference) assignment.columnName()).getName().toString(),
              SqlParserPos.ZERO),
          new SqlIdentifier(assignment.expression().toString(), SqlParserPos.ZERO));
    }
  }

  @Override
  public SqlNode visitShowSessionParameter(ShowSessionParameter node, Void context) {
    return new SqlShowParameters(
        SqlParserPos.ZERO, new SqlIdentifier(node.parameter().toString(), SqlParserPos.ZERO));
  }

  @Override
  protected SqlNode visitNotExpression(NotExpression node, Void context) {
    var inner = node.getValue();
    if (inner instanceof LikePredicate) {
      var notLikeOperator = lookupOperator("NOT LIKE", SqlSyntax.SPECIAL);
      var likeCall = visitLikePredicate((LikePredicate) inner, context);
      return new SqlBasicCall(notLikeOperator, likeCall.getOperands(), SqlParserPos.ZERO);
    } else if (inner instanceof InPredicate) {
      var notInOperator = lookupOperator("NOT IN", SqlSyntax.BINARY);
      var inCall = visitInPredicate((InPredicate) inner, context);
      return new SqlBasicCall(notInOperator, inCall.getOperands(), SqlParserPos.ZERO);
    } else if (inner instanceof ExistsPredicate) {
      var notOperator = lookupOperator("NOT", SqlSyntax.PREFIX);
      var existsOperator = visitExists((ExistsPredicate) inner, context);
      return new SqlBasicCall(notOperator, new SqlNode[] {existsOperator}, SqlParserPos.ZERO);
    } else {
      var notOperator = lookupOperator("NOT", SqlSyntax.PREFIX);
      var childExpression = inner.accept(this, context);
      return new SqlBasicCall(notOperator, new SqlNode[] {childExpression}, SqlParserPos.ZERO);
    }
  }

  @Override
  protected SqlNode visitBooleanComparisonExpression(
      BooleanComparisonExpression node, Void context) {
    var sqlOperator = lookupOperator(node.getComparisonType().getFunctionName(), SqlSyntax.POSTFIX);
    var input = node.getChild().accept(this, context);
    return new SqlBasicCall(sqlOperator, new SqlNode[] {input}, SqlParserPos.ZERO);
  }

  @Override
  protected SqlNode visitIsNotNullPredicate(IsNotNullPredicate node, Void context) {
    var operator = lookupOperator("IS NOT NULL", SqlSyntax.POSTFIX);
    var childExpression = node.getValue().accept(this, context);
    return new SqlBasicCall(operator, new SqlNode[] {childExpression}, SqlParserPos.ZERO);
  }

  @Override
  protected SqlNode visitIsNullPredicate(IsNullPredicate node, Void context) {
    var operator = lookupOperator("IS NULL", SqlSyntax.POSTFIX);
    var childExpression = node.getValue().accept(this, context);
    return new SqlBasicCall(operator, new SqlNode[] {childExpression}, SqlParserPos.ZERO);
  }

  private static SqlBasicTypeNameSpec toBasicTypeNameSpec(ColumnType<?> columnType) {
    String typeName = columnType.name().toUpperCase();
    switch (typeName) {
      case "BOOLEAN":
      case "BOOL":
        return new SqlBasicTypeNameSpec(SqlTypeName.BOOLEAN, SqlParserPos.ZERO);
      case "SMALLINT":
      case "INT2":
        return new SqlBasicTypeNameSpec(SqlTypeName.SMALLINT, SqlParserPos.ZERO);
      case "INT":
      case "INT4":
      case "INTEGER":
        return new SqlBasicTypeNameSpec(SqlTypeName.INTEGER, SqlParserPos.ZERO);
      case "BIGINT":
      case "INT8":
        return new SqlBasicTypeNameSpec(SqlTypeName.BIGINT, SqlParserPos.ZERO);
      case "FLOAT":
      case "REAL":
      case "FLOAT4":
        return new SqlBasicTypeNameSpec(SqlTypeName.FLOAT, SqlParserPos.ZERO);
      case "DOUBLE":
      case "DOUBLE PRECISION":
      case "FLOAT8":
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
            return new SqlBasicTypeNameSpec(SqlTypeName.VARCHAR, 1, SqlParserPos.ZERO);
          } else {
            return new SqlBasicTypeNameSpec(
                SqlTypeName.VARCHAR, parameters.get(0), SqlParserPos.ZERO);
          }
        }
      case "TEXT":
        // According to https://www.postgresql.org/docs/9.5/datatype-character.html
        // TEXT should be varchar without size limit, and by default varchar without size should be
        // size 1
        return new SqlBasicTypeNameSpec(
            SqlTypeName.VARCHAR, StringType.MAX_SIZE, SqlParserPos.ZERO);
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
      case "TIMESTAMPZ":
        return new SqlBasicTypeNameSpec(
            SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE, SqlParserPos.ZERO);
      default:
        throw new PgException(PgErrorCode.SYNTAX_ERROR, "Unsupported type name: %s", typeName);
    }
  }

  private SqlOperator lookupOperator(String functionName, SqlSyntax syntax) {
    return lookupOperator(new SqlIdentifier(functionName, SqlParserPos.ZERO), syntax);
  }

  private SqlOperator lookupOperator(SqlIdentifier functionName, SqlSyntax syntax) {
    return operatorTable.lookupOneOperator(functionName, syntax);
  }
}
