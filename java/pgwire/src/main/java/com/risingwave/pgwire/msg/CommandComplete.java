package com.risingwave.pgwire.msg;

public final class CommandComplete {

  /**
   * @param stmtType TODO(TaoWu): Use an enum instead of String.
   * @param rowsCnt The number of rows being effected. <br>
   *     <ul>
   *       <li>For a DELETE command, rowsCnt is the number of rows deleted.
   *       <li>For an UPDATE command, rowsCnt is the number of rows updated.
   *       <li>For an INSERT command, rowsCnt is the number of rows inserted.
   *       <li>For an SELECT command, rowsCnt is the number of rows retrieved.
   *     </ul>
   */
  public CommandComplete(String stmtType, int rowsCnt) {
    tag = stmtType;
    if (stmtType.equals("INSERT")) {
      tag += " 0"; // Oid. Always 0 since the oid system in postgres is deprecated.
    }
    if (stmtType.equals("INSERT")
        || stmtType.equals("DELETE")
        || stmtType.equals("UPDATE")
        || stmtType.equals("SELECT")
        || stmtType.equals("MOVE")
        || stmtType.equals("FETCH")
        || stmtType.equals("COPY")) {
      tag += " " + rowsCnt;
    }
  }

  public String getTag() {
    return tag;
  }

  private String tag;
}
