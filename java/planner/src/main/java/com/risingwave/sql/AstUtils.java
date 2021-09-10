package com.risingwave.sql;

import com.risingwave.sql.tree.QualifiedName;
import java.util.Collection;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParserPos;

public class AstUtils {
  public static SqlIdentifier identifierOf(QualifiedName name) {
    return new SqlIdentifier(name.getParts(), SqlParserPos.ZERO);
  }

  public static SqlIdentifier identifierOf(String name) {
    return new SqlIdentifier(name, SqlParserPos.ZERO);
  }

  public static SqlNodeList sqlNodeListOf(Collection<SqlNode> nodes) {
    return new SqlNodeList(nodes, SqlParserPos.ZERO);
  }
}
