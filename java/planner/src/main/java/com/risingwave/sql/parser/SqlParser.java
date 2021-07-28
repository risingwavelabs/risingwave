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

import static java.util.Objects.requireNonNull;

import com.risingwave.sql.ToCalciteAstVisitor;
import com.risingwave.sql.parser.antlr.v4.SqlBaseBaseListener;
import com.risingwave.sql.parser.antlr.v4.SqlBaseLexer;
import com.risingwave.sql.parser.antlr.v4.SqlBaseParser;
import com.risingwave.sql.tree.AstVisitor;
import com.risingwave.sql.tree.Expression;
import com.risingwave.sql.tree.Node;
import com.risingwave.sql.tree.Statement;
import java.util.EnumSet;
import java.util.function.Function;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonToken;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.antlr.v4.runtime.misc.Pair;
import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.antlr.v4.runtime.tree.TerminalNodeImpl;
import org.apache.calcite.sql.SqlNode;

public class SqlParser {
  private static final BaseErrorListener ERROR_LISTENER =
      new BaseErrorListener() {
        @Override
        public void syntaxError(
            Recognizer<?, ?> recognizer,
            Object offendingSymbol,
            int line,
            int charPositionInLine,
            String message,
            RecognitionException e) {
          throw new ParsingException(message, e, line, charPositionInLine);
        }
      };

  public static final SqlParser INSTANCE = new SqlParser();

  private final EnumSet<IdentifierSymbol> allowedIdentifierSymbols;

  public SqlParser() {
    this(new SqlParserOptions().allowIdentifierSymbol(IdentifierSymbol.AT_SIGN));
  }

  public SqlParser(SqlParserOptions options) {
    requireNonNull(options, "options is null");
    allowedIdentifierSymbols = EnumSet.copyOf(options.getAllowedIdentifierSymbols());
  }

  public static SqlNode toCalciteAst(Node node) {
    AstVisitor<SqlNode, Void> converter = new ToCalciteAstVisitor();
    return node.accept(converter, null);
  }

  public static SqlNode createCalciteStatement(String sql) {
    return toCalciteAst(createStatement(sql));
  }

  public static SqlNode createCalciteExpression(String sql) {
    return toCalciteAst(createExpression(sql));
  }

  public static Statement createStatement(String sql) {
    return INSTANCE.generateStatement(sql);
  }

  private Statement generateStatement(String sql) {
    return (Statement) invokeParser("statement", sql, SqlBaseParser::singleStatement);
  }

  public static Expression createExpression(String expression) {
    return INSTANCE.generateExpression(expression);
  }

  private Expression generateExpression(String expression) {
    return (Expression) invokeParser("expression", expression, SqlBaseParser::singleExpression);
  }

  private Node invokeParser(
      String name, String sql, Function<SqlBaseParser, ParserRuleContext> parseFunction) {
    try {
      SqlBaseLexer lexer =
          new SqlBaseLexer(new CaseInsensitiveStream(CharStreams.fromString(sql, name)));
      CommonTokenStream tokenStream = new CommonTokenStream(lexer);
      SqlBaseParser parser = new SqlBaseParser(tokenStream);

      parser.addParseListener(new PostProcessor());

      lexer.removeErrorListeners();
      lexer.addErrorListener(ERROR_LISTENER);

      parser.removeErrorListeners();
      parser.addErrorListener(ERROR_LISTENER);

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
    } catch (StackOverflowError e) {
      throw new ParsingException(name + " is too large (stack overflow while parsing)");
    }
  }

  private class PostProcessor extends SqlBaseBaseListener {
    @Override
    public void exitUnquotedIdentifier(SqlBaseParser.UnquotedIdentifierContext context) {
      String identifier = context.IDENTIFIER().getText();
      for (IdentifierSymbol identifierSymbol : EnumSet.complementOf(allowedIdentifierSymbols)) {
        char symbol = identifierSymbol.getSymbol();
        if (identifier.indexOf(symbol) >= 0) {
          throw new ParsingException(
              "identifiers must not contain '" + identifierSymbol.getSymbol() + "'",
              null,
              context.IDENTIFIER().getSymbol().getLine(),
              context.IDENTIFIER().getSymbol().getCharPositionInLine());
        }
      }
    }

    @Override
    public void exitBackQuotedIdentifier(SqlBaseParser.BackQuotedIdentifierContext context) {
      Token token = context.BACKQUOTED_IDENTIFIER().getSymbol();
      throw new ParsingException(
          "backquoted identifiers are not supported; use double quotes to quote identifiers",
          null,
          token.getLine(),
          token.getCharPositionInLine());
    }

    @Override
    public void exitDigitIdentifier(SqlBaseParser.DigitIdentifierContext context) {
      Token token = context.DIGIT_IDENTIFIER().getSymbol();
      throw new ParsingException(
          "identifiers must not start with a digit; surround the identifier with double quotes",
          null,
          token.getLine(),
          token.getCharPositionInLine());
    }

    @Override
    public void exitColonIdentifier(SqlBaseParser.ColonIdentifierContext context) {
      Token token = context.COLON_IDENT().getSymbol();
      throw new ParsingException(
          "identifiers must not contain ':'", null, token.getLine(), token.getCharPositionInLine());
    }

    @Override
    public void exitNonReserved(SqlBaseParser.NonReservedContext context) {
      // replace nonReserved words with IDENT tokens
      context.getParent().removeLastChild();

      Token token = (Token) context.getChild(0).getPayload();
      context
          .getParent()
          .addChild(
              new TerminalNodeImpl(
                  new CommonToken(
                      new Pair<>(token.getTokenSource(), token.getInputStream()),
                      SqlBaseLexer.IDENTIFIER,
                      token.getChannel(),
                      token.getStartIndex(),
                      token.getStopIndex())));
    }
  }
}
