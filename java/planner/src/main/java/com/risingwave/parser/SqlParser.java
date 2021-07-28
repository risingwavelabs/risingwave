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

package com.risingwave.parser;

import com.risingwave.sql.parser.antlr.v4.SqlBaseLexer;
import com.risingwave.sql.parser.antlr.v4.SqlBaseParser;
import java.util.function.Function;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.apache.calcite.sql.SqlNode;

public class SqlParser {

  private static final SqlParser INSTANCE = new SqlParser();

  private SqlParser() {}

  public static SqlNode createStatement(String sql) {
    return INSTANCE.generateStatement(sql);
  }

  private SqlNode generateStatement(String sql) {
    return invokeParser("statement", sql, SqlBaseParser::singleStatement);
  }

  private SqlNode invokeParser(
      String name, String sql, Function<SqlBaseParser, ParserRuleContext> parseFunction) {
    // TODO: Add exception handle.
    SqlBaseLexer lexer =
        new SqlBaseLexer(new CaseInsensitiveStream(CharStreams.fromString(sql, name)));
    CommonTokenStream tokenStream = new CommonTokenStream(lexer);
    SqlBaseParser parser = new SqlBaseParser(tokenStream);
    ParserRuleContext tree;
    try {
      // first, try parsing with potentially faster SLL mode
      parser.getInterpreter().setPredictionMode(PredictionMode.SLL);
      tree = parseFunction.apply(parser);
    } catch (ParseCancellationException ex) {
      // if we fail, parse with LL mode
      tokenStream.seek(0); // rewind input stream
      parser.reset();

      parser.getInterpreter().setPredictionMode(PredictionMode.LL);
      tree = parseFunction.apply(parser);
    }

    return new AstBuilder().visit(tree);
  }
}
