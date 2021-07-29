package com.risingwave.sql;

import static com.google.common.base.Verify.verify;

import com.risingwave.common.exception.PgErrorCode;
import com.risingwave.common.exception.PgException;
import com.risingwave.sql.tree.AllColumns;
import com.risingwave.sql.tree.AstVisitor;
import com.risingwave.sql.tree.ColumnDefinition;
import com.risingwave.sql.tree.ColumnType;
import com.risingwave.sql.tree.CreateTable;
import com.risingwave.sql.tree.DoubleLiteral;
import com.risingwave.sql.tree.DropTable;
import com.risingwave.sql.tree.Insert;
import com.risingwave.sql.tree.IntegerLiteral;
import com.risingwave.sql.tree.LongLiteral;
import com.risingwave.sql.tree.Node;
import com.risingwave.sql.tree.NotNullColumnConstraint;
import com.risingwave.sql.tree.Query;
import com.risingwave.sql.tree.QuerySpecification;
import com.risingwave.sql.tree.StringLiteral;
import com.risingwave.sql.tree.Table;
import com.risingwave.sql.tree.Values;
import com.risingwave.sql.tree.ValuesList;
import java.math.BigDecimal;
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
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.ddl.SqlDdlNodes;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;

public class ToCalciteAstVisitor extends AstVisitor<SqlNode, Void> {
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

    if (notNull) {
      dataTypeSpec = dataTypeSpec.withNullable(Boolean.FALSE);
    }

    ColumnStrategy columnStrategy = notNull ? ColumnStrategy.NOT_NULLABLE : ColumnStrategy.NULLABLE;

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

    return new SqlSelect(
        SqlParserPos.ZERO, null, selectList, from, null, null, null, null, null, null, null, null);
  }

  @Override
  public SqlNode visitDropTable(DropTable<?> node, Void context) {
    SqlIdentifier tableName = visitTable(node.table(), context);
    return SqlDdlNodes.dropTable(SqlParserPos.ZERO, false, tableName);
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

  private static SqlBasicTypeNameSpec toBasicTypeNameSpec(ColumnType<?> columnType) {
    String typeName = columnType.name().toUpperCase();
    switch (typeName) {
      case "INT":
      case "INTEGER":
        return new SqlBasicTypeNameSpec(SqlTypeName.INTEGER, SqlParserPos.ZERO);
      case "FLOAT":
      case "REAL":
        return new SqlBasicTypeNameSpec(SqlTypeName.FLOAT, SqlParserPos.ZERO);
      case "DOUBLE PRECISION":
        return new SqlBasicTypeNameSpec(SqlTypeName.DOUBLE, SqlParserPos.ZERO);
      default:
        throw new PgException(PgErrorCode.SYNTAX_ERROR, "Unsupported type name: %s", typeName);
    }
  }

  public static void main(String[] args) throws SqlParseException {
    SqlNode node =
        SqlParser.create("insert into t values(1,4,2), (2,3,3), (3,4,4), (4,3,5)").parseStmt();
    System.out.println(node);
  }
}
