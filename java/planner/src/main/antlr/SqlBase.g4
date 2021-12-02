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

grammar SqlBase;

@header {
    package com.risingwave.sql.parser.antlr.v4;
}

singleStatement
    : statement SEMICOLON? EOF
    ;

singleExpression
    : expr EOF
    ;

statement
    : query                                                                          #default
    | BEGIN (WORK | TRANSACTION)? (transactionMode (',' transactionMode)*)?          #begin
    | START TRANSACTION (transactionMode (',' transactionMode)*)?                    #startTransaction
    | COMMIT                                                                         #commit
    | EXPLAIN (ANALYZE)? statement                                                   #explain
    | OPTIMIZE TABLE tableWithPartitions withProperties?                             #optimize
    | REFRESH TABLE tableWithPartitions                                              #refreshTable
    | UPDATE aliasedRelation
        SET assignment (',' assignment)*
        where?
        returning?                                                                   #update
    | DELETE FROM aliasedRelation where?                                             #delete
    | SHOW (TRANSACTION ISOLATION LEVEL | TRANSACTION_ISOLATION)                     #showTransaction
    | SHOW CREATE TABLE table                                                        #showCreateTable
    | SHOW TABLES ((FROM | IN) qname)? (LIKE pattern=stringLiteral | where)?         #showTables
    | SHOW SCHEMAS (LIKE pattern=stringLiteral | where)?                             #showSchemas
    | SHOW COLUMNS (FROM | IN) tableName=qname ((FROM | IN) schema=qname)?
        (LIKE pattern=stringLiteral | where)?                                        #showColumns
    | SHOW (qname | ALL)                                                             #showSessionParameter
    | ALTER TABLE alterTableDefinition ADD COLUMN? addColumnDefinition               #addColumn
    | ALTER TABLE alterTableDefinition DROP CONSTRAINT ident                         #dropCheckConstraint
    | ALTER TABLE alterTableDefinition
        (SET '(' genericProperties ')' | RESET ('(' ident (',' ident)* ')')?)        #alterTableProperties
    | ALTER TABLE alterTableDefinition (OPEN | CLOSE)                        #alterTableOpenClose
    | ALTER TABLE alterTableDefinition RENAME TO qname                       #alterTableRename
    | ALTER TABLE alterTableDefinition REROUTE rerouteOption                 #alterTableReroute
    | ALTER CLUSTER REROUTE RETRY FAILED                                             #alterClusterRerouteRetryFailed
    | ALTER CLUSTER SWAP TABLE source=qname TO target=qname withProperties?          #alterClusterSwapTable
    | ALTER CLUSTER DECOMMISSION node=expr                                           #alterClusterDecommissionNode
    | ALTER CLUSTER GC DANGLING ARTIFACTS                                            #alterClusterGCDanglingArtifacts
    | ALTER USER name=ident SET '(' genericProperties ')'                            #alterUser
    | RESET GLOBAL primaryExpression (',' primaryExpression)*                        #resetGlobal
    | SET (SESSION CHARACTERISTICS AS)? TRANSACTION
        transactionMode (',' transactionMode)*                                       #setTransaction
    | SET (SESSION | LOCAL)? SESSION AUTHORIZATION
        (DEFAULT | username=stringLiteralOrIdentifier)                               #setSessionAuthorization
    | RESET SESSION AUTHORIZATION                                                    #resetSessionAuthorization
    | SET (SESSION | LOCAL)? qname
        (EQ | TO) (DEFAULT | setExpr (',' setExpr)*)                                 #set
    | SET GLOBAL (PERSISTENT | TRANSIENT)?
        setGlobalAssignment (',' setGlobalAssignment)*                               #setGlobal
    | SET LICENSE stringLiteral                                                      #setLicense
    | KILL (ALL | jobId=parameterOrString)                                           #kill
    | INSERT INTO table ('(' ident (',' ident)* ')')? insertSource
        onConflict?
        returning?                                                                   #insert
    | RESTORE SNAPSHOT qname
        (ALL | METADATA | TABLE tableWithPartitions | metatypes=idents)
        withProperties?                                                              #restore
    | COPY tableWithPartition FROM path=expr withProperties? (RETURN SUMMARY)?       #copyFrom
    | COPY tableWithPartition columns? where?
        TO DIRECTORY? path=expr withProperties?                                      #copyTo
    | DROP (TABLE | STREAM) (IF EXISTS)? table                                       #dropTable
    | DROP ALIAS qname                                                               #dropAlias
    | DROP SNAPSHOT qname                                                            #dropSnapshot
    | DROP FUNCTION (IF EXISTS)? name=qname
        '(' (functionArgument (',' functionArgument)*)? ')'                          #dropFunction
    | DROP USER (IF EXISTS)? name=ident                                              #dropUser
    | DROP (MATERIALIZED)? VIEW (IF EXISTS)? name=qname                                            #dropView
    | DROP ANALYZER name=ident                                                       #dropAnalyzer
    | GRANT (priviliges=idents | ALL PRIVILEGES?)
        (ON clazz qnames)? TO users=idents                                           #grantPrivilege
    | DENY (priviliges=idents | ALL PRIVILEGES?)
        (ON clazz qnames)? TO users=idents                                           #denyPrivilege
    | REVOKE (privileges=idents | ALL PRIVILEGES?)
        (ON clazz qnames)? FROM users=idents                                         #revokePrivilege
    | createStmt                                                                     #create
    | DEALLOCATE (PREPARE)? (ALL | prepStmt=stringLiteralOrIdentifierOrQname)        #deallocate
    | ANALYZE                                                                        #analyze
    | DISCARD (ALL | PLANS | SEQUENCES | TEMPORARY | TEMP)                           #discard
    ;

query:
      queryTerm
      (ORDER BY sortItem (',' sortItem)*)?
      (LIMIT limit=parameterOrInteger)?
      (OFFSET offset=parameterOrInteger)?
    ;

queryTerm
    : querySpec                                                                      #queryTermDefault
    | first=querySpec operator=(INTERSECT | EXCEPT) second=querySpec                 #setOperation
    | left=queryTerm operator=UNION setQuant? right=queryTerm                        #setOperation
    ;

setQuant
    : DISTINCT
    | ALL
    ;

sortItem
    : expr ordering=(ASC | DESC)? (NULLS nullOrdering=(FIRST | LAST))?
    ;

querySpec
    : SELECT setQuant? selectItem (',' selectItem)*
      (FROM relation (',' relation)*)?
      where?
      (GROUP BY expr (',' expr)*)?
      (HAVING having=booleanExpression)?
      (WINDOW windows+=namedWindow (',' windows+=namedWindow)*)?                     #defaultQuerySpec
    | VALUES values (',' values)*                                                    #valuesRelation
    ;

selectItem
    : expr (AS? ident)?                                                              #selectSingle
    | qname '.' ASTERISK                                                             #selectAll
    | ASTERISK                                                                       #selectAll
    ;

where
    : WHERE condition=booleanExpression
    ;

returning
    : RETURNING selectItem (',' selectItem)*
    ;

filter
    : FILTER '(' where ')'
    ;

relation
    : left=relation
      ( CROSS JOIN right=aliasedRelation
      | joinType JOIN rightRelation=relation joinCriteria
      | NATURAL joinType JOIN right=aliasedRelation
      )                                                                              #joinRelation
    | aliasedRelation                                                                #relationDefault
    ;

joinType
    : INNER?
    | LEFT OUTER?
    | RIGHT OUTER?
    | FULL OUTER?
    ;

joinCriteria
    : ON booleanExpression
    | USING '(' ident (',' ident)* ')'
    ;

aliasedRelation
    : relationPrimary (AS? ident aliasedColumns?)?
    ;

relationPrimary
    : table                                                                          #tableRelation
    | '(' query ')'                                                                  #subqueryRelation
    | '(' relation ')'                                                               #parenthesizedRelation
    ;

tableWithPartition
    : qname ( PARTITION '(' assignment ( ',' assignment )* ')')?
    ;

table
    : qname                                                                          #tableName
    | qname '(' valueExpression? (',' valueExpression)* ')'                          #tableFunction
    ;

aliasedColumns
    : '(' ident (',' ident)* ')'
    ;

expr
    : booleanExpression
    ;

booleanExpression
    : predicated                                                                     #booleanDefault
    | NOT booleanExpression                                                          #logicalNot
    | left=booleanExpression operator=AND right=booleanExpression                    #logicalBinary
    | left=booleanExpression operator=OR right=booleanExpression                     #logicalBinary
    | MATCH '(' matchPredicateIdents ',' term=primaryExpression ')'
        (USING matchType=ident withProperties?)?                                     #match
    ;

predicated
    : valueExpression predicate[$valueExpression.ctx]?
    ;

predicate[ParserRuleContext value]
    : cmpOp right=valueExpression                                                    #comparison
    | cmpOp setCmpQuantifier primaryExpression                                       #quantifiedComparison
    | NOT? BETWEEN lower=valueExpression AND upper=valueExpression                   #between
    | NOT? IN '(' expr (',' expr)* ')'                                               #inList
    | NOT? IN subqueryExpression                                                     #inSubquery
    | NOT? (LIKE | ILIKE) pattern=valueExpression (ESCAPE escape=valueExpression)?   #like
    | NOT? (LIKE | ILIKE) quant=setCmpQuantifier '(' v=valueExpression')'
        (ESCAPE escape=valueExpression)?                                             #arrayLike
    | IS NOT? NULL                                                                   #nullPredicate
    | IS NOT? DISTINCT FROM right=valueExpression                                    #distinctFrom
    ;

valueExpression
    : primaryExpression                                                              #valueExpressionDefault
    | operator=(MINUS | PLUS) valueExpression                                        #arithmeticUnary
    | left=valueExpression operator=(ASTERISK | SLASH | PERCENT)
        right=valueExpression                                                        #arithmeticBinary
    | left=valueExpression operator=(PLUS | MINUS) right=valueExpression             #arithmeticBinary
    | left=valueExpression CONCAT right=valueExpression                              #concatenation
    | dataType stringLiteral                                                         #fromStringLiteralCast
    ;

primaryExpression
    : parameterOrLiteral                                                             #defaultParamOrLiteral
    | explicitFunction                                                               #explicitFunctionDefault
    | qname '(' ASTERISK ')' filter? over?                                           #functionCall
    | ident                                                                          #columnReference
    | qname '(' (setQuant? expr (',' expr)*)? ')' filter? over?                      #functionCall
    | subqueryExpression                                                             #subqueryExpressionDefault
    | '(' base=primaryExpression ')' '.' fieldName=ident                             #recordSubscript
    | '(' expr ')'                                                                   #nestedExpression
    // This is an extension to ANSI SQL, which considers EXISTS to be a <boolean expression>
    | EXISTS '(' query ')'                                                           #exists
    | value=primaryExpression '[' index=valueExpression ']'                          #subscript
    | ident ('.' ident)*                                                             #dereference
    | primaryExpression CAST_OPERATOR dataType                                       #doubleColonCast
    | timestamp=primaryExpression AT TIME ZONE zone=primaryExpression                #atTimezone
    | 'ARRAY'? '[]'                                                                  #emptyArray
    ;

explicitFunction
    : name=CURRENT_DATE                                                              #specialDateTimeFunction
    | name=CURRENT_TIME ('(' precision=integerLiteral')')?                           #specialDateTimeFunction
    | name=CURRENT_TIMESTAMP ('(' precision=integerLiteral')')?                      #specialDateTimeFunction
    | CURRENT_SCHEMA                                                                 #currentSchema
    | (CURRENT_USER | USER)                                                          #currentUser
    | SESSION_USER                                                                   #sessionUser
    | LEFT '(' strOrColName=expr ',' len=expr ')'                                    #left
    | RIGHT '(' strOrColName=expr ',' len=expr ')'                                   #right
    | SUBSTRING '(' expr FROM expr (FOR expr)? ')'                                   #substring
    | TRIM '(' ((trimMode=(LEADING | TRAILING | BOTH))?
                (charsToTrim=expr)? FROM)? target=expr ')'                           #trim
    | EXTRACT '(' stringLiteralOrIdentifier FROM expr ')'                            #extract
    | CAST '(' expr AS dataType ')'                                                  #cast
    | TRY_CAST '(' expr AS dataType ')'                                              #cast
    | CASE operand=expr whenClause+ (ELSE elseExpr=expr)? END                        #simpleCase
    | CASE whenClause+ (ELSE elseExpr=expr)? END                                     #searchedCase
    | IF '('condition=expr ',' trueValue=expr (',' falseValue=expr)? ')'             #ifCase
    | ARRAY subqueryExpression                                                       #arraySubquery
    ;

subqueryExpression
    : '(' query ')'
    ;

parameterOrLiteral
    : parameterOrSimpleLiteral                            #simpleLiteral
    | ARRAY? '[' (expr (',' expr)*)? ']'                  #arrayLiteral
    | '{' (objectKeyValue (',' objectKeyValue)*)? '}'     #objectLiteral
    ;

parameterOrSimpleLiteral
    : nullLiteral
    | intervalLiteral
    | escapedCharsStringLiteral
    | stringLiteral
    | numericLiteral
    | booleanLiteral
    | bitString
    | parameterExpr
    ;

parameterOrInteger
    : parameterExpr
    | integerLiteral
    ;

parameterOrIdent
    : parameterExpr
    | ident
    ;

parameterOrString
    : parameterExpr
    | stringLiteral
    ;

parameterExpr
    : '$' integerLiteral                                                             #positionalParameter
    | '?'                                                                            #parameterPlaceholder
    ;

nullLiteral
    : NULL
    ;

escapedCharsStringLiteral
    : ESCAPED_STRING
    ;

stringLiteral
    : STRING
    ;

bitString
    : BIT_STRING
    ;

subscriptSafe
    : value=subscriptSafe '[' index=valueExpression']'
    | qname
    ;

cmpOp
    : EQ | NEQ | LT | LTE | GT | GTE | LLT | REGEX_MATCH | REGEX_NO_MATCH | REGEX_MATCH_CI | REGEX_NO_MATCH_CI
    ;

setCmpQuantifier
    : ANY | SOME | ALL
    ;

whenClause
    : WHEN condition=expr THEN result=expr
    ;

namedWindow
    : name=ident AS windowDefinition
    ;

over
    : OVER windowDefinition
    ;

windowDefinition
    : windowRef=ident
    | '('
        (windowRef=ident)?
        (PARTITION BY partition+=expr (',' partition+=expr)*)?
        (ORDER BY sortItem (',' sortItem)*)?
        windowFrame?
      ')'
    ;

windowFrame
    : frameType=RANGE start=frameBound
    | frameType=ROWS start=frameBound
    | frameType=RANGE BETWEEN start=frameBound AND end=frameBound
    | frameType=ROWS BETWEEN start=frameBound AND end=frameBound
    ;

frameBound
    : UNBOUNDED boundType=PRECEDING                 #unboundedFrame
    | UNBOUNDED boundType=FOLLOWING                 #unboundedFrame
    | CURRENT ROW                                   #currentRowBound
    | expr boundType=(PRECEDING | FOLLOWING)        #boundedFrame
    ;

qnames
    : qname (',' qname)*
    ;

qname
    : ident ('.' ident)*
    ;

idents
    : ident (',' ident)*
    ;

ident
    : unquotedIdent
    | quotedIdent
    ;

unquotedIdent
    : IDENTIFIER                        #unquotedIdentifier
    | nonReserved                       #unquotedIdentifier
    | DIGIT_IDENTIFIER                  #digitIdentifier        // not supported
    | COLON_IDENT                       #colonIdentifier        // not supported
    ;

quotedIdent
    : QUOTED_IDENTIFIER                 #quotedIdentifier
    | BACKQUOTED_IDENTIFIER             #backQuotedIdentifier   // not supported
    ;

stringLiteralOrIdentifier
    : ident
    | stringLiteral
    ;

stringLiteralOrIdentifierOrQname
    : ident
    | qname
    | stringLiteral
    ;

numericLiteral
    : decimalLiteral
    | integerLiteral
    ;

intervalLiteral
    : INTERVAL sign=(PLUS | MINUS)? stringLiteral from=intervalField (TO to=intervalField)?
    ;

intervalField
    : YEAR | MONTH | DAY | HOUR | MINUTE | SECOND
    ;

booleanLiteral
    : TRUE
    | FALSE
    ;

decimalLiteral
    : DECIMAL_VALUE
    ;

integerLiteral
    : INTEGER_VALUE
    ;

objectKeyValue
    : key=ident EQ value=expr
    ;

insertSource
   : query
   | '(' query ')'
   ;

onConflict
   : ON CONFLICT conflictTarget? DO NOTHING
   | ON CONFLICT conflictTarget DO UPDATE SET assignment (',' assignment)*
   ;

conflictTarget
   : '(' subscriptSafe (',' subscriptSafe)* ')'
   ;

values
    : '(' expr (',' expr)* ')'
    ;

columns
    : '(' primaryExpression (',' primaryExpression)* ')'
    ;

assignment
    : primaryExpression EQ expr
    ;

createStmt
    : CREATE TABLE (IF NOT EXISTS)? table
        '(' tableElement (',' tableElement)* ')'
         partitionedByOrClusteredInto withProperties?                                #createTable
    | CREATE TABLE table AS insertSource                                             #createTableAs
    | CREATE SNAPSHOT qname (ALL | TABLE tableWithPartitions) withProperties?        #createSnapshot
    | CREATE ANALYZER name=ident (EXTENDS extendedName=ident)?
        WITH? '(' analyzerElement ( ',' analyzerElement )* ')'                       #createAnalyzer
    | CREATE (OR REPLACE)? FUNCTION name=qname
        '(' (functionArgument (',' functionArgument)*)? ')'
        RETURNS returnType=dataType
        LANGUAGE language=parameterOrIdent
        AS body=parameterOrString                                                    #createFunction
    | CREATE USER name=ident withProperties?                                         #createUser
    | CREATE ( OR REPLACE )? (MATERIALIZED)? VIEW name=qname AS query                #createView
    | CREATE ( OR REPLACE )? STREAM (IF NOT EXISTS)? name=ident
        '(' tableElement (',' tableElement)* ')'
        withProperties?
        ROW FORMAT rowFormat=stringLiteralOrIdentifier
        (ROW SCHEMA LOCATION rowSchemaLocation=stringLiteral)?                       #createStream
    ;

functionArgument
    : (name=ident)? type=dataType
    ;

alterTableDefinition
    : ONLY qname                                                                     #tableOnly
    | tableWithPartition                                                             #tableWithPartitionDefault
    ;

partitionedByOrClusteredInto
    : partitionedBy? clusteredBy?
    | clusteredBy? partitionedBy?
    ;

partitionedBy
    : PARTITIONED BY columns
    ;

clusteredBy
    : CLUSTERED (BY '(' routing=primaryExpression ')')?
        (INTO numShards=parameterOrInteger SHARDS)?
    ;

tableElement
    : columnDefinition                                                               #columnDefinitionDefault
    | PRIMARY_KEY columns                                                            #primaryKeyConstraint
    | INDEX name=ident USING method=ident columns withProperties?                    #indexDefinition
    | checkConstraint                                                                #tableCheckConstraint
    ;

columnDefinition
    : ident dataType? (DEFAULT defaultExpr=expr)? ((GENERATED ALWAYS)? AS generatedExpr=expr)? columnConstraint*
    ;

addColumnDefinition
    : subscriptSafe dataType? ((GENERATED ALWAYS)? AS expr)? columnConstraint*
    ;

rerouteOption
    : MOVE SHARD shardId=parameterOrInteger FROM fromNodeId=parameterOrString TO toNodeId=parameterOrString #rerouteMoveShard
    | ALLOCATE REPLICA SHARD shardId=parameterOrInteger ON nodeId=parameterOrString                         #rerouteAllocateReplicaShard
    | PROMOTE REPLICA SHARD shardId=parameterOrInteger ON nodeId=parameterOrString withProperties?          #reroutePromoteReplica
    | CANCEL SHARD shardId=parameterOrInteger ON nodeId=parameterOrString withProperties?                   #rerouteCancelShard
    ;

dataType
    : baseDataType
        ('(' integerLiteral (',' integerLiteral )* ')')?    #maybeParametrizedDataType
    | objectTypeDefinition                                  #objectDataType
    | ARRAY '(' dataType ')'                                #arrayDataType
    | dataType '[]'                                         #arrayDataType
    ;

baseDataType
    : definedDataType   #definedDataTypeDefault
    | ident             #identDataType
    ;

definedDataType
    : DOUBLE PRECISION
    | TIMESTAMP WITHOUT TIME ZONE
    | TIMESTAMP WITH TIME ZONE
    | TIME WITH TIME ZONE
    | CHARACTER VARYING
    | BOOL
    ;

objectTypeDefinition
    : OBJECT ('(' type=(DYNAMIC | STRICT | IGNORED) ')')?
        (AS '(' columnDefinition ( ',' columnDefinition )* ')')?
    ;

columnConstraint
    : PRIMARY_KEY                                                                    #columnConstraintPrimaryKey
    | NOT NULL                                                                       #columnConstraintNotNull
    | INDEX USING method=ident withProperties?                                       #columnIndexConstraint
    | INDEX OFF                                                                      #columnIndexOff
    | STORAGE withProperties                                                         #columnStorageDefinition
    | checkConstraint                                                                #columnCheckConstraint
    ;

checkConstraint
    : (CONSTRAINT name=ident)? CHECK '(' expression=booleanExpression ')'
    ;

withProperties
    : WITH '(' genericProperties ')'                                                 #withGenericProperties
    ;

genericProperties
    : genericProperty (',' genericProperty)*
    ;

genericProperty
    : stringLiteralOrIdentifierOrQname EQ expr
    ;

matchPredicateIdents
    : matchPred=matchPredicateIdent
    | '(' matchPredicateIdent (',' matchPredicateIdent)* ')'
    ;

matchPredicateIdent
    : subscriptSafe boost=parameterOrSimpleLiteral?
    ;

analyzerElement
    : tokenizer
    | tokenFilters
    | charFilters
    | genericProperty
    ;

tokenizer
    : TOKENIZER namedProperties
    ;

tokenFilters
    : TOKEN_FILTERS '(' namedProperties (',' namedProperties )* ')'
    ;

charFilters
    : CHAR_FILTERS '(' namedProperties (',' namedProperties )* ')'
    ;

namedProperties
    : ident withProperties?
    ;

tableWithPartitions
    : tableWithPartition (',' tableWithPartition)*
    ;

setGlobalAssignment
    : name=primaryExpression (EQ | TO) value=expr
    ;

setExpr
    : stringLiteral
    | booleanLiteral
    | numericLiteral
    | ident
    | on
    ;

on
    : ON
    ;

clazz
    : SCHEMA
    | TABLE
    | VIEW
    ;

transactionMode
    : ISOLATION LEVEL isolationLevel
    | (READ WRITE | READ ONLY)
    | (NOT)? DEFERRABLE
    ;

isolationLevel
    : SERIALIZABLE
    | REPEATABLE READ
    | READ COMMITTED
    | READ UNCOMMITTED
    ;

nonReserved
    : ALIAS | ANALYZE | ANALYZER | AT | AUTHORIZATION | BERNOULLI | CATALOGS | CHAR_FILTERS | CHECK | CLUSTERED
    | COLUMNS | COPY | CURRENT |  DAY | DEALLOCATE | DISTRIBUTED | DUPLICATE | DYNAMIC | EXPLAIN
    | EXTENDS | FOLLOWING | FORMAT | FULLTEXT | FUNCTIONS | GEO_POINT | GEO_SHAPE | GLOBAL
    | GRAPHVIZ | HOUR | IGNORED | ILIKE | INTERVAL | KEY | KILL | LICENSE | LOGICAL | LOCAL
    | MATERIALIZED | MINUTE | MONTH | OFF | ONLY | OVER | OPTIMIZE | PARTITION | PARTITIONED
    | PARTITIONS | PLAIN | PRECEDING | RANGE | REFRESH | ROW | ROWS | SCHEMAS | SECOND | SESSION
    | SHARDS | SHOW | STORAGE | STRICT | SYSTEM | TABLES | TABLESAMPLE | TEXT | TIME | ZONE | WITHOUT
    | TIMESTAMP | TO | TOKENIZER | TOKEN_FILTERS | TYPE | VALUES | VIEW | YEAR
    | SNAPSHOT | RESTORE | GENERATED | ALWAYS | BEGIN | START | COMMIT
    | ISOLATION | TRANSACTION | CHARACTERISTICS | LEVEL | LANGUAGE | OPEN | CLOSE | RENAME
    | PRIVILEGES | SCHEMA | PREPARE
    | REROUTE | MOVE | SHARD | ALLOCATE | REPLICA | CANCEL | CLUSTER | RETRY | FAILED | FILTER
    | DO | NOTHING | CONFLICT | TRANSACTION_ISOLATION | RETURN | SUMMARY
    | WORK | SERIALIZABLE | REPEATABLE | COMMITTED | UNCOMMITTED | READ | WRITE | WINDOW | DEFERRABLE
    | STRING_TYPE | IP | DOUBLE | FLOAT | TIMESTAMP | LONG | INT | INTEGER | SHORT | BYTE | BOOLEAN | PRECISION
    | REPLACE | RETURNING | SWAP | GC | DANGLING | ARTIFACTS | DECOMMISSION | LEADING | TRAILING | BOTH | TRIM
    | CURRENT_SCHEMA | PROMOTE | CHARACTER | VARYING
    | DISCARD | PLANS | SEQUENCES | TEMPORARY | TEMP | METADATA
    | STREAM | LOCATION | TRUE | FALSE
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
TABLE: 'TABLE';
STREAM: 'STREAM';
SWAP: 'SWAP';
GC: 'GC';
DANGLING: 'DANGLING';
ARTIFACTS: 'ARTIFACTS';
DECOMMISSION: 'DECOMMISSION';
CLUSTER: 'CLUSTER';
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

BOOL: 'BOOL';
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

LOCATION: 'LOCATION';

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
