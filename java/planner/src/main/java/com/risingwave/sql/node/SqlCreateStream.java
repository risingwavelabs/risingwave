package com.risingwave.sql.node;

import static java.util.Objects.requireNonNull;

import com.risingwave.sql.tree.ColumnDefinition;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlCreate;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

/** The calcite node for CREATE STREAM statement. */
public class SqlCreateStream extends SqlCreate {

  private final SqlIdentifier name;
  private final SqlNodeList columnList;
  private final SqlNodeList propertyList;
  private final SqlCharStringLiteral rowFormat;
  private final SqlCharStringLiteral rowSchemaLocation;
  private final List<ColumnDefinition<?>> primaryColumns;


  public SqlIdentifier getName() {
    return name;
  }

  public SqlNodeList getColumnList() {
    return columnList;
  }

  public SqlNodeList getPropertyList() {
    return propertyList;
  }

  public SqlCharStringLiteral getRowFormat() {
    return rowFormat;
  }

  public SqlCharStringLiteral getRowSchemaLocation() {
    return rowSchemaLocation;
  }

  private static final SqlOperator OPERATOR =
      new SqlSpecialOperator("CREATE STREAM", SqlKind.OTHER_DDL);

  public SqlCreateStream(
      SqlParserPos pos,
      SqlIdentifier name,
      @Nullable SqlNodeList columnList,
      SqlNodeList propertyList,
      SqlCharStringLiteral rowFormat,
      SqlCharStringLiteral rowSchemaLocation,
      List<ColumnDefinition<?>> primaryColumns) {
    super(OPERATOR, pos, false, true);
    this.name = requireNonNull(name, "name");
    this.columnList = columnList;
    this.propertyList = requireNonNull(propertyList, "propertyList");
    this.rowFormat = requireNonNull(rowFormat, "rowFormat");
    this.rowSchemaLocation = requireNonNull(rowSchemaLocation, "rowSchemaLocation");
    this.primaryColumns = primaryColumns;
  }

  @Override
  public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(name, columnList, propertyList, rowFormat);
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("CREATE");
    writer.keyword("STREAM");
    if (ifNotExists) {
      writer.keyword("IF NOT EXISTS");
    }
    name.unparse(writer, leftPrec, rightPrec);
    if (columnList != null) {
      SqlWriter.Frame frame = writer.startList("(", ")");
      for (SqlNode c : columnList) {
        writer.sep(",");
        c.unparse(writer, 0, 0);
      }
      writer.endList(frame);
    }
    writer.keyword("WITH");
    SqlWriter.Frame frame = writer.startList("(", ")");
    if (propertyList != null) {
      propertyList.forEach(node -> node.unparse(writer, leftPrec, rightPrec));
    }
    writer.endList(frame);
    writer.keyword("ROW FORMAT");
    writer.literal(rowFormat.toValue());
  }

  public List<ColumnDefinition<?>> getPrimaryColumns() {
    return primaryColumns;
  }
}
