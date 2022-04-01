package com.risingwave.pgwire.database;

public enum TransactionStatus {
  IDLE('I'), // Not in a transaction block
  BLOCK('T'), // In a transaction block
  FAIL('E'); // In a failed transaction

  TransactionStatus(char c) {
    this.status = c;
  }

  public char asChar() {
    return status;
  }

  private final char status;
}
