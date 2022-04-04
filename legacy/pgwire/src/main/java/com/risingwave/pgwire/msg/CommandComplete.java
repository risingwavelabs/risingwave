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
  public CommandComplete(StatementType stmtType, int rowsCnt) {
    StringBuilder sb = new StringBuilder();
    sb.append(stmtType.name());
    if (stmtType == StatementType.INSERT) {
      sb.append(" 0"); // Oid. Always 0 since the oid system in postgres is deprecated.
    }

    switch (stmtType) {
      case INSERT:
      case DELETE:
      case UPDATE:
      case SELECT:
      case MOVE:
      case FETCH:
      case COPY:
        sb.append(" ").append(rowsCnt);
        break;
      default:
        break;
    }

    this.tag = sb.toString();
  }

  public String getTag() {
    return tag;
  }

  private final String tag;
}
