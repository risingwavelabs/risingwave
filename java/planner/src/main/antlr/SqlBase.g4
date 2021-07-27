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

// Minimal grammar file, only include what we need from Crate 2.0.
grammar SqlBase;

@header {
    package com.risingwave.parser.antlr.v4;
}

singleStatement
    : statement SEMICOLON? EOF
    ;

statement
    : query                                                                          #default
    | createStmt                                                                     #create
    ;

createStmt
    : CREATE TABLE (IF NOT EXISTS)? table
                '(' tableElement (',' tableElement)* ')'                           #createTable
    | INSERT INTO table ('(' ident (',' ident)* ')')? insertSource                 #insert


    ;

insertSource
   : query
   | '(' query ')'
   ;

query
    : queryTerm
    ;

queryTerm
    : querySpec                                                                      #queryTermDefault
    ;

querySpec
    : SELECT setQuant? selectItem (',' selectItem)*
      (FROM relation (',' relation)*)?
      where?
      (GROUP BY expr (',' expr)*)?
      (HAVING having=booleanExpression)?                    #defaultQuerySpec
    | VALUES values (',' values)*                                                    #valuesRelation
    ;

relation
//    : left=relation
//      ( CROSS JOIN right=aliasedRelation
//      | joinType JOIN rightRelation=relation joinCriteria
//      | NATURAL joinType JOIN right=aliasedRelation
//      )                                                                              #joinRelation
    : aliasedRelation                                                                #relationDefault
    ;

aliasedRelation
    : relationPrimary (AS? ident )?
    ;

relationPrimary
    : table                                                                          #tableRelation
//    | '(' query ')'                                                                  #subqueryRelation
//    | '(' relation ')'                                                               #parenthesizedRelation
    ;

setQuant
    : DISTINCT
    | ALL
    ;

selectItem
    : expr (AS? ident)?                                                              #selectSingle
//    | qname '.' ASTERISK                                                             #selectAll
    | ASTERISK                                                                       #selectAll
    ;

values
    : '(' expr (',' expr)* ')'
    ;

where
    : WHERE condition=booleanExpression
    ;

expr
    : booleanExpression
    ;

booleanExpression
    : predicated                                                                     #booleanDefault
    ;

predicated
    : valueExpression predicate[$valueExpression.ctx]?
    ;

predicate[ParserRuleContext value]
    : cmpOp right=valueExpression                                                    #comparison
    ;

cmpOp
    : EQ | NEQ | LT | LTE | GT | GTE | LLT | REGEX_MATCH | REGEX_NO_MATCH | REGEX_MATCH_CI | REGEX_NO_MATCH_CI
    ;

valueExpression
    : primaryExpression                                                              #valueExpressionDefault
    ;

primaryExpression
    : parameterOrLiteral                                                             #defaultParamOrLiteral
    | ident                                                                          #columnReference
    ;

parameterOrLiteral
    : parameterOrSimpleLiteral                                                     #simpleLiteral
    ;

parameterOrSimpleLiteral
    : numericLiteral
    ;

numericLiteral
    : integerLiteral
    ;
table
    : qname                                                                        #tableName
    ;

qname
    : ident ('.' ident)*
    ;

tableElement
    : columnDefinition                  #columnDefinitionDefault
    ;

columnDefinition
    : ident dataType?  columnConstraint*
    ;

columnConstraint
    : PRIMARY_KEY                                                                    #columnConstraintPrimaryKey
    | NOT NULL                                                                       #columnConstraintNotNull
    ;

dataType
    : baseDataType
            ('(' integerLiteral (',' integerLiteral )* ')')?    #maybeParametrizedDataType
    ;

baseDataType
    : ident             #identDataType
    ;


ident
    : unquotedIdent
    ;

unquotedIdent
    : IDENTIFIER                        #unquotedIdentifier
    | nonReserved                       #unquotedIdentifier
    ;

integerLiteral
    : INTEGER_VALUE
    ;

nonReserved
    : ALIAS | ANALYZE | ANALYZER | AT | AUTHORIZATION | BERNOULLI | BLOB | CATALOGS | CHAR_FILTERS | CHECK | CLUSTERED
    | COLUMNS | COPY | CURRENT |  DAY | DEALLOCATE | DISTRIBUTED | DUPLICATE | DYNAMIC | EXPLAIN
    | EXTENDS | FOLLOWING | FORMAT | FULLTEXT | FUNCTIONS | GEO_POINT | GEO_SHAPE | GLOBAL
    | GRAPHVIZ | HOUR | IGNORED | ILIKE | INTERVAL | KEY | KILL | LICENSE | LOGICAL | LOCAL
    | MATERIALIZED | MINUTE | MONTH | OFF | ONLY | OVER | OPTIMIZE | PARTITION | PARTITIONED
    | PARTITIONS | PLAIN | PRECEDING | RANGE | REFRESH | ROW | ROWS | SCHEMAS | SECOND | SESSION
    | SHARDS | SHOW | STORAGE | STRICT | SYSTEM | TABLES | TABLESAMPLE | TEXT | TIME | ZONE | WITHOUT
    | TIMESTAMP | TO | TOKENIZER | TOKEN_FILTERS | TYPE | VALUES | VIEW | YEAR
    | REPOSITORY | SNAPSHOT | RESTORE | GENERATED | ALWAYS | BEGIN | START | COMMIT
    | ISOLATION | TRANSACTION | CHARACTERISTICS | LEVEL | LANGUAGE | OPEN | CLOSE | RENAME
    | PRIVILEGES | SCHEMA | PREPARE
    | REROUTE | MOVE | SHARD | ALLOCATE | REPLICA | CANCEL | CLUSTER | RETRY | FAILED | FILTER
    | DO | NOTHING | CONFLICT | TRANSACTION_ISOLATION | RETURN | SUMMARY
    | WORK | SERIALIZABLE | REPEATABLE | COMMITTED | UNCOMMITTED | READ | WRITE | WINDOW | DEFERRABLE
    | STRING_TYPE | IP | DOUBLE | FLOAT | TIMESTAMP | LONG | INT | INTEGER | SHORT | BYTE | BOOLEAN | PRECISION
    | REPLACE | RETURNING | SWAP | GC | DANGLING | ARTIFACTS | DECOMMISSION | LEADING | TRAILING | BOTH | TRIM
    | CURRENT_SCHEMA | PROMOTE | CHARACTER | VARYING
    | DISCARD | PLANS | SEQUENCES | TEMPORARY | TEMP | METADATA
    ;

AUTHORIZATION: 'AUTHORIZATION';
SELECT: 'SELECT';
FROM: 'FROM';
TO: 'TO';
AS: 'AS';
AT: 'AT';
ALL: 'ALL';
ANY: 'ANY';
SOME: 'SOME';
DEALLOCATE: 'DEALLOCATE';
DIRECTORY: 'DIRECTORY';
DISTINCT: 'DISTINCT';
WHERE: 'WHERE';
GROUP: 'GROUP';
BY: 'BY';
ORDER: 'ORDER';
HAVING: 'HAVING';
LIMIT: 'LIMIT';
OFFSET: 'OFFSET';
OR: 'OR';
AND: 'AND';
IN: 'IN';
NOT: 'NOT';
EXISTS: 'EXISTS';
BETWEEN: 'BETWEEN';
LIKE: 'LIKE';
ILIKE: 'ILIKE';
IS: 'IS';
NULL: 'NULL';
TRUE: 'TRUE';
FALSE: 'FALSE';
NULLS: 'NULLS';
FIRST: 'FIRST';
LAST: 'LAST';
ESCAPE: 'ESCAPE';
ASC: 'ASC';
DESC: 'DESC';
SUBSTRING: 'SUBSTRING';
TRIM: 'TRIM';
LEADING: 'LEADING';
TRAILING: 'TRAILING';
BOTH: 'BOTH';
FOR: 'FOR';
TIME: 'TIME';
ZONE: 'ZONE';
YEAR: 'YEAR';
MONTH: 'MONTH';
DAY: 'DAY';
HOUR: 'HOUR';
MINUTE: 'MINUTE';
SECOND: 'SECOND';
CURRENT_DATE: 'CURRENT_DATE';
CURRENT_TIME: 'CURRENT_TIME';
CURRENT_TIMESTAMP: 'CURRENT_TIMESTAMP';
CURRENT_SCHEMA: 'CURRENT_SCHEMA';
CURRENT_USER: 'CURRENT_USER';
SESSION_USER: 'SESSION_USER';
EXTRACT: 'EXTRACT';
CASE: 'CASE';
WHEN: 'WHEN';
THEN: 'THEN';
ELSE: 'ELSE';
END: 'END';
IF: 'IF';
INTERVAL: 'INTERVAL';
JOIN: 'JOIN';
CROSS: 'CROSS';
OUTER: 'OUTER';
INNER: 'INNER';
LEFT: 'LEFT';
RIGHT: 'RIGHT';
FULL: 'FULL';
NATURAL: 'NATURAL';
USING: 'USING';
ON: 'ON';
OVER: 'OVER';
WINDOW: 'WINDOW';
PARTITION: 'PARTITION';
PROMOTE: 'PROMOTE';
RANGE: 'RANGE';
ROWS: 'ROWS';
UNBOUNDED: 'UNBOUNDED';
PRECEDING: 'PRECEDING';
FOLLOWING: 'FOLLOWING';
CURRENT: 'CURRENT';
ROW: 'ROW';
WITH: 'WITH';
WITHOUT: 'WITHOUT';
RECURSIVE: 'RECURSIVE';
CREATE: 'CREATE';
BLOB: 'BLOB';
TABLE: 'TABLE';
SWAP: 'SWAP';
GC: 'GC';
DANGLING: 'DANGLING';
ARTIFACTS: 'ARTIFACTS';
DECOMMISSION: 'DECOMMISSION';
CLUSTER: 'CLUSTER';
REPOSITORY: 'REPOSITORY';
SNAPSHOT: 'SNAPSHOT';
ALTER: 'ALTER';
KILL: 'KILL';
ONLY: 'ONLY';

ADD: 'ADD';
COLUMN: 'COLUMN';

OPEN: 'OPEN';
CLOSE: 'CLOSE';

RENAME: 'RENAME';

REROUTE: 'REROUTE';
MOVE: 'MOVE';
SHARD: 'SHARD';
ALLOCATE: 'ALLOCATE';
REPLICA: 'REPLICA';
CANCEL: 'CANCEL';
RETRY: 'RETRY';
FAILED: 'FAILED';

BOOLEAN: 'BOOLEAN';
BYTE: 'BYTE';
SHORT: 'SHORT';
INTEGER: 'INTEGER';
INT: 'INT';
LONG: 'LONG';
FLOAT: 'FLOAT';
DOUBLE: 'DOUBLE';
PRECISION: 'PRECISION';
TIMESTAMP: 'TIMESTAMP';
IP: 'IP';
CHARACTER: 'CHARACTER';
VARYING: 'VARYING';
OBJECT: 'OBJECT';
STRING_TYPE: 'STRING';
GEO_POINT: 'GEO_POINT';
GEO_SHAPE: 'GEO_SHAPE';
GLOBAL : 'GLOBAL';
SESSION : 'SESSION';
LOCAL : 'LOCAL';
LICENSE : 'LICENSE';

BEGIN: 'BEGIN';
START: 'START';
COMMIT: 'COMMIT';
WORK: 'WORK';
TRANSACTION: 'TRANSACTION';
TRANSACTION_ISOLATION: 'TRANSACTION_ISOLATION';
CHARACTERISTICS: 'CHARACTERISTICS';
ISOLATION: 'ISOLATION';
LEVEL: 'LEVEL';
SERIALIZABLE: 'SERIALIZABLE';
REPEATABLE: 'REPEATABLE';
COMMITTED: 'COMMITTED';
UNCOMMITTED: 'UNCOMMITTED';
READ: 'READ';
WRITE: 'WRITE';
DEFERRABLE: 'DEFERRABLE';

RETURNS: 'RETURNS';
CALLED: 'CALLED';
REPLACE: 'REPLACE';
FUNCTION: 'FUNCTION';
LANGUAGE: 'LANGUAGE';
INPUT: 'INPUT';

ANALYZE: 'ANALYZE';
DISCARD: 'DISCARD';
PLANS: 'PLANS';
SEQUENCES: 'SEQUENCES';
TEMPORARY: 'TEMPORARY';
TEMP: 'TEMP';
CONSTRAINT: 'CONSTRAINT';
CHECK: 'CHECK';
DESCRIBE: 'DESCRIBE';
EXPLAIN: 'EXPLAIN';
FORMAT: 'FORMAT';
TYPE: 'TYPE';
TEXT: 'TEXT';
GRAPHVIZ: 'GRAPHVIZ';
LOGICAL: 'LOGICAL';
DISTRIBUTED: 'DISTRIBUTED';
CAST: 'CAST';
TRY_CAST: 'TRY_CAST';
SHOW: 'SHOW';
TABLES: 'TABLES';
SCHEMAS: 'SCHEMAS';
CATALOGS: 'CATALOGS';
COLUMNS: 'COLUMNS';
PARTITIONS: 'PARTITIONS';
FUNCTIONS: 'FUNCTIONS';
MATERIALIZED: 'MATERIALIZED';
VIEW: 'VIEW';
OPTIMIZE: 'OPTIMIZE';
REFRESH: 'REFRESH';
RESTORE: 'RESTORE';
DROP: 'DROP';
ALIAS: 'ALIAS';
UNION: 'UNION';
EXCEPT: 'EXCEPT';
INTERSECT: 'INTERSECT';
SYSTEM: 'SYSTEM';
BERNOULLI: 'BERNOULLI';
TABLESAMPLE: 'TABLESAMPLE';
STRATIFY: 'STRATIFY';
INSERT: 'INSERT';
INTO: 'INTO';
VALUES: 'VALUES';
DELETE: 'DELETE';
UPDATE: 'UPDATE';
KEY: 'KEY';
DUPLICATE: 'DUPLICATE';
CONFLICT: 'CONFLICT';
DO: 'DO';
NOTHING: 'NOTHING';
SET: 'SET';
RESET: 'RESET';
DEFAULT: 'DEFAULT';
COPY: 'COPY';
CLUSTERED: 'CLUSTERED';
SHARDS: 'SHARDS';
PRIMARY_KEY: 'PRIMARY KEY';
OFF: 'OFF';
FULLTEXT: 'FULLTEXT';
FILTER: 'FILTER';
PLAIN: 'PLAIN';
INDEX: 'INDEX';
STORAGE: 'STORAGE';
RETURNING: 'RETURNING';

DYNAMIC: 'DYNAMIC';
STRICT: 'STRICT';
IGNORED: 'IGNORED';

ARRAY: 'ARRAY';

ANALYZER: 'ANALYZER';
EXTENDS: 'EXTENDS';
TOKENIZER: 'TOKENIZER';
TOKEN_FILTERS: 'TOKEN_FILTERS';
CHAR_FILTERS: 'CHAR_FILTERS';

PARTITIONED: 'PARTITIONED';
PREPARE: 'PREPARE';

TRANSIENT: 'TRANSIENT';
PERSISTENT: 'PERSISTENT';

MATCH: 'MATCH';

GENERATED: 'GENERATED';
ALWAYS: 'ALWAYS';

USER: 'USER';
GRANT: 'GRANT';
DENY: 'DENY';
REVOKE: 'REVOKE';
PRIVILEGES: 'PRIVILEGES';
SCHEMA: 'SCHEMA';

RETURN: 'RETURN';
SUMMARY: 'SUMMARY';

METADATA: 'METADATA';

EQ  : '=';
NEQ : '<>' | '!=';
LT  : '<';
LTE : '<=';
GT  : '>';
GTE : '>=';
LLT  : '<<';
REGEX_MATCH: '~';
REGEX_NO_MATCH: '!~';
REGEX_MATCH_CI: '~*';
REGEX_NO_MATCH_CI: '!~*';

PLUS: '+';
MINUS: '-';
ASTERISK: '*';
SLASH: '/';
PERCENT: '%';
CONCAT: '||';
CAST_OPERATOR: '::';
SEMICOLON: ';';

STRING
    : '\'' ( ~'\'' | '\'\'' )* '\''
    ;

ESCAPED_STRING
    : 'E' '\'' ( ~'\'' | '\'\'' | '\\\'' )* '\''
    ;

BIT_STRING
    : 'B' '\'' ([0-1])* '\''
    ;


INTEGER_VALUE
    : DIGIT+
    ;

DECIMAL_VALUE
    : DIGIT+ '.' DIGIT*
    | '.' DIGIT+
    | DIGIT+ ('.' DIGIT*)? EXPONENT
    | '.' DIGIT+ EXPONENT
    ;

IDENTIFIER
    : (LETTER | '_') (LETTER | DIGIT | '_' | '@')*
    ;

DIGIT_IDENTIFIER
    : DIGIT (LETTER | DIGIT | '_' | '@')+
    ;

QUOTED_IDENTIFIER
    : '"' ( ~'"' | '""' )* '"'
    ;

BACKQUOTED_IDENTIFIER
    : '`' ( ~'`' | '``' )* '`'
    ;

COLON_IDENT
    : (LETTER | DIGIT | '_' )+ ':' (LETTER | DIGIT | '_' )+
    ;

fragment EXPONENT
    : 'E' [+-]? DIGIT+
    ;

fragment DIGIT
    : [0-9]
    ;

fragment LETTER
    : [A-Za-z]
    ;

COMMENT
    : ('--' ~[\r\n]* '\r'? '\n'? | '/*' .*? '*/') -> channel(HIDDEN)
    ;

WS
    : [ \r\n\t]+ -> channel(HIDDEN)
    ;

UNRECOGNIZED
    : .
    ;