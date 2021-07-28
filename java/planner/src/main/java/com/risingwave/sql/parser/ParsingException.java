/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package com.risingwave.sql.parser;

import static java.lang.String.format;

import java.util.Locale;
import org.antlr.v4.runtime.RecognitionException;

public class ParsingException extends RuntimeException {

  private final int line;
  private final int charPositionInLine;

  ParsingException(String message, RecognitionException cause, int line, int charPositionInLine) {
    super(message, cause);

    this.line = line;
    this.charPositionInLine = charPositionInLine;
  }

  ParsingException(String message) {
    this(message, null, 1, 0);
  }

  int getLineNumber() {
    return line;
  }

  int getColumnNumber() {
    return charPositionInLine + 1;
  }

  public String getErrorMessage() {
    return super.getMessage();
  }

  @Override
  public String getMessage() {
    return format(
        Locale.ENGLISH, "line %s:%s: %s", getLineNumber(), getColumnNumber(), getErrorMessage());
  }
}
