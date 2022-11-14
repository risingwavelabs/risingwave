// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Test SQL syntax specific to PostgreSQL. The parser based on the
//! generic dialect is also tested (on the inputs it can handle).

#[macro_use]
mod test_utils;
use risingwave_sqlparser::ast::*;
use risingwave_sqlparser::parser::ParserError;
use test_utils::*;

#[test]
fn parse_create_table_with_defaults() {
    let sql = "CREATE TABLE public.customer (
            customer_id integer DEFAULT nextval(public.customer_customer_id_seq),
            store_id smallint NOT NULL,
            first_name character varying NOT NULL,
            last_name character varying COLLATE \"es_ES\" NOT NULL,
            email character varying,
            address_id smallint NOT NULL,
            activebool boolean DEFAULT true NOT NULL,
            create_date date DEFAULT now()::text NOT NULL,
            last_update timestamp without time zone DEFAULT now() NOT NULL,
            active integer NOT NULL
    ) WITH (fillfactor = 20, user_catalog_table = true, autovacuum_vacuum_threshold = 100)";
    match one_statement_parses_to(sql, "") {
        Statement::CreateTable {
            name,
            columns,
            constraints,
            with_options,
            if_not_exists: false,
            ..
        } => {
            assert_eq!("public.customer", name.to_string());
            assert_eq!(
                columns,
                vec![
                    ColumnDef::new(
                        "customer_id".into(),
                        DataType::Int(None),
                        None,
                        vec![ColumnOptionDef {
                            name: None,
                            option: ColumnOption::Default(verified_expr(
                                "nextval(public.customer_customer_id_seq)"
                            ))
                        }],
                    ),
                    ColumnDef::new(
                        "store_id".into(),
                        DataType::SmallInt(None),
                        None,
                        vec![ColumnOptionDef {
                            name: None,
                            option: ColumnOption::NotNull,
                        }],
                    ),
                    ColumnDef::new(
                        "first_name".into(),
                        DataType::Varchar,
                        None,
                        vec![ColumnOptionDef {
                            name: None,
                            option: ColumnOption::NotNull,
                        }],
                    ),
                    ColumnDef::new(
                        "last_name".into(),
                        DataType::Varchar,
                        Some(ObjectName(vec![Ident::with_quote('"', "es_ES")])),
                        vec![ColumnOptionDef {
                            name: None,
                            option: ColumnOption::NotNull,
                        }],
                    ),
                    ColumnDef::new("email".into(), DataType::Varchar, None, vec![],),
                    ColumnDef::new(
                        "address_id".into(),
                        DataType::SmallInt(None),
                        None,
                        vec![ColumnOptionDef {
                            name: None,
                            option: ColumnOption::NotNull
                        }],
                    ),
                    ColumnDef::new(
                        "activebool".into(),
                        DataType::Boolean,
                        None,
                        vec![
                            ColumnOptionDef {
                                name: None,
                                option: ColumnOption::Default(Expr::Value(Value::Boolean(true))),
                            },
                            ColumnOptionDef {
                                name: None,
                                option: ColumnOption::NotNull,
                            }
                        ],
                    ),
                    ColumnDef::new(
                        "create_date".into(),
                        DataType::Date,
                        None,
                        vec![
                            ColumnOptionDef {
                                name: None,
                                option: ColumnOption::Default(verified_expr("CAST(now() AS TEXT)"))
                            },
                            ColumnOptionDef {
                                name: None,
                                option: ColumnOption::NotNull,
                            }
                        ],
                    ),
                    ColumnDef::new(
                        "last_update".into(),
                        DataType::Timestamp(false),
                        None,
                        vec![
                            ColumnOptionDef {
                                name: None,
                                option: ColumnOption::Default(verified_expr("now()")),
                            },
                            ColumnOptionDef {
                                name: None,
                                option: ColumnOption::NotNull,
                            }
                        ],
                    ),
                    ColumnDef::new(
                        "active".into(),
                        DataType::Int(None),
                        None,
                        vec![ColumnOptionDef {
                            name: None,
                            option: ColumnOption::NotNull
                        }],
                    ),
                ]
            );
            assert!(constraints.is_empty());
            assert_eq!(
                with_options,
                vec![
                    SqlOption {
                        name: vec!["fillfactor".into()].into(),
                        value: number("20")
                    },
                    SqlOption {
                        name: vec!["user_catalog_table".into()].into(),
                        value: Value::Boolean(true)
                    },
                    SqlOption {
                        name: vec!["autovacuum_vacuum_threshold".into()].into(),
                        value: number("100")
                    },
                ]
            );
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_create_table_from_pg_dump() {
    let sql = "CREATE TABLE public.customer (
            customer_id integer DEFAULT nextval('public.customer_customer_id_seq'::regclass) NOT NULL,
            store_id smallint NOT NULL,
            first_name character varying NOT NULL,
            last_name character varying NOT NULL,
            info text[],
            address_id smallint NOT NULL,
            activebool boolean DEFAULT true NOT NULL,
            create_date date DEFAULT now()::DATE NOT NULL,
            create_date1 date DEFAULT 'now'::TEXT::date NOT NULL,
            last_update timestamp without time zone DEFAULT now(),
            release_year public.year,
            active integer
        )";
    one_statement_parses_to(sql, "CREATE TABLE public.customer (\
            customer_id INT DEFAULT nextval(CAST('public.customer_customer_id_seq' AS REGCLASS)) NOT NULL, \
            store_id SMALLINT NOT NULL, \
            first_name CHARACTER VARYING NOT NULL, \
            last_name CHARACTER VARYING NOT NULL, \
            info TEXT[], \
            address_id SMALLINT NOT NULL, \
            activebool BOOLEAN DEFAULT true NOT NULL, \
            create_date DATE DEFAULT CAST(now() AS DATE) NOT NULL, \
            create_date1 DATE DEFAULT CAST(CAST('now' AS TEXT) AS DATE) NOT NULL, \
            last_update TIMESTAMP DEFAULT now(), \
            release_year public.year, \
            active INT\
        )");
}

#[test]
fn parse_create_table_with_inherit() {
    let sql = "\
               CREATE TABLE bazaar.settings (\
               settings_id UUID PRIMARY KEY DEFAULT uuid_generate_v4() NOT NULL, \
               user_id UUID UNIQUE, \
               value TEXT[], \
               use_metric BOOLEAN DEFAULT true\
               )";
    verified_stmt(sql);
}

#[test]
fn parse_create_table_empty() {
    // Zero-column tables are weird, but supported by at least PostgreSQL.
    // <https://github.com/sqlparser-rs/sqlparser-rs/pull/94>
    let _ = verified_stmt("CREATE TABLE t ()");
}

#[test]
fn parse_create_table_constraints_only() {
    // Zero-column tables can also have constraints in PostgreSQL
    let sql = "CREATE TABLE t (CONSTRAINT positive CHECK (2 > 1))";
    let ast = verified_stmt(sql);
    match ast {
        Statement::CreateTable {
            name,
            columns,
            constraints,
            ..
        } => {
            assert_eq!("t", name.to_string());
            assert!(columns.is_empty());
            assert_eq!(
                only(constraints).to_string(),
                "CONSTRAINT positive CHECK (2 > 1)"
            );
        }
        _ => unreachable!(),
    };
}

#[test]
fn parse_alter_table_constraints_rename() {
    match verified_stmt("ALTER TABLE tab RENAME CONSTRAINT old_name TO new_name") {
        Statement::AlterTable {
            name,
            operation: AlterTableOperation::RenameConstraint { old_name, new_name },
        } => {
            assert_eq!("tab", name.to_string());
            assert_eq!(old_name.to_string(), "old_name");
            assert_eq!(new_name.to_string(), "new_name");
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_alter_table_alter_column() {
    one_statement_parses_to(
        "ALTER TABLE tab ALTER COLUMN is_active TYPE TEXT USING 'text'",
        "ALTER TABLE tab ALTER COLUMN is_active SET DATA TYPE TEXT USING 'text'",
    );

    match verified_stmt("ALTER TABLE tab ALTER COLUMN is_active SET DATA TYPE TEXT USING 'text'") {
        Statement::AlterTable {
            name,
            operation: AlterTableOperation::AlterColumn { column_name, op },
        } => {
            assert_eq!("tab", name.to_string());
            assert_eq!("is_active", column_name.to_string());
            let using_expr = Expr::Value(Value::SingleQuotedString("text".to_string()));
            assert_eq!(
                op,
                AlterColumnOperation::SetDataType {
                    data_type: DataType::Text,
                    using: Some(using_expr),
                }
            );
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_create_table_if_not_exists() {
    let sql = "CREATE TABLE IF NOT EXISTS uk_cities ()";
    let ast = verified_stmt(sql);
    match ast {
        Statement::CreateTable {
            name,
            if_not_exists: true,
            ..
        } => {
            assert_eq!("uk_cities", name.to_string());
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_bad_if_not_exists() {
    let res = parse_sql_statements("CREATE TABLE NOT EXISTS uk_cities ()");
    assert_eq!(
        ParserError::ParserError("Expected end of statement, found: EXISTS".to_string()),
        res.unwrap_err()
    );

    let res = parse_sql_statements("CREATE TABLE IF EXISTS uk_cities ()");
    assert_eq!(
        ParserError::ParserError("Expected end of statement, found: EXISTS".to_string()),
        res.unwrap_err()
    );

    let res = parse_sql_statements("CREATE TABLE IF uk_cities ()");
    assert_eq!(
        ParserError::ParserError("Expected end of statement, found: uk_cities".to_string()),
        res.unwrap_err()
    );

    let res = parse_sql_statements("CREATE TABLE IF NOT uk_cities ()");
    assert_eq!(
        ParserError::ParserError("Expected end of statement, found: NOT".to_string()),
        res.unwrap_err()
    );
}

#[test]
fn parse_create_schema_if_not_exists() {
    let sql = "CREATE SCHEMA IF NOT EXISTS schema_name";
    let ast = verified_stmt(sql);
    match ast {
        Statement::CreateSchema {
            if_not_exists: true,
            schema_name,
        } => assert_eq!("schema_name", schema_name.to_string()),
        _ => unreachable!(),
    }
}

#[test]
fn parse_drop_schema_if_exists() {
    let sql = "DROP SCHEMA IF EXISTS schema_name";
    let ast = verified_stmt(sql);
    match ast {
        Statement::Drop(stmt) => assert_eq!(stmt.object_type, ObjectType::Schema),
        _ => unreachable!(),
    }
}

#[test]
fn parse_copy_example() {
    let sql = r#"COPY public.actor (actor_id, first_name, last_name, last_update, value) FROM stdin;
1	PENELOPE	GUINNESS	2006-02-15 09:34:33 0.11111
2	NICK	WAHLBERG	2006-02-15 09:34:33 0.22222
3	ED	CHASE	2006-02-15 09:34:33 0.312323
4	JENNIFER	DAVIS	2006-02-15 09:34:33 0.3232
5	JOHNNY	LOLLOBRIGIDA	2006-02-15 09:34:33 1.343
6	BETTE	NICHOLSON	2006-02-15 09:34:33 5.0
7	GRACE	MOSTEL	2006-02-15 09:34:33 6.0
8	MATTHEW	JOHANSSON	2006-02-15 09:34:33 7.0
9	JOE	SWANK	2006-02-15 09:34:33 8.0
10	CHRISTIAN	GABLE	2006-02-15 09:34:33 9.1
11	ZERO	CAGE	2006-02-15 09:34:33 10.001
12	KARL	BERRY	2017-11-02 19:15:42.308637+08 11.001
A Fateful Reflection of a Waitress And a Boat who must Discover a Sumo Wrestler in Ancient China
Kwara & Kogi
{"Deleted Scenes","Behind the Scenes"}
'awe':5 'awe-inspir':4 'barbarella':1 'cat':13 'conquer':16 'dog':18 'feminist':10 'inspir':6 'monasteri':21 'must':15 'stori':7 'streetcar':2
PHP	₱ USD $
\N  Some other value
\\."#;
    let ast = one_statement_parses_to(sql, "");
    println!("{:#?}", ast);
    // assert_eq!(sql, ast.to_string());
}

#[test]
fn parse_set() {
    let stmt = verified_stmt("SET a = b");
    assert_eq!(
        stmt,
        Statement::SetVariable {
            local: false,
            variable: "a".into(),
            value: vec![SetVariableValue::Ident("b".into())],
        }
    );

    let stmt = verified_stmt("SET a = 'b'");
    assert_eq!(
        stmt,
        Statement::SetVariable {
            local: false,
            variable: "a".into(),
            value: vec![SetVariableValue::Literal(Value::SingleQuotedString(
                "b".into()
            ))],
        }
    );

    let stmt = verified_stmt("SET a = 0");
    assert_eq!(
        stmt,
        Statement::SetVariable {
            local: false,
            variable: "a".into(),
            value: vec![SetVariableValue::Literal(number("0"))],
        }
    );

    let stmt = verified_stmt("SET a = DEFAULT");
    assert_eq!(
        stmt,
        Statement::SetVariable {
            local: false,
            variable: "a".into(),
            value: vec![SetVariableValue::Ident("DEFAULT".into())],
        }
    );

    let stmt = verified_stmt("SET LOCAL a = b");
    assert_eq!(
        stmt,
        Statement::SetVariable {
            local: true,
            variable: "a".into(),
            value: vec![SetVariableValue::Ident("b".into())],
        }
    );

    one_statement_parses_to("SET a TO b", "SET a = b");
    one_statement_parses_to("SET SESSION a = b", "SET a = b");

    assert_eq!(
        parse_sql_statements("SET"),
        Err(ParserError::ParserError(
            "Expected identifier, found: EOF".to_string()
        )),
    );

    assert_eq!(
        parse_sql_statements("SET a b"),
        Err(ParserError::ParserError(
            "Expected equals sign or TO, found: b".to_string()
        )),
    );

    assert_eq!(
        parse_sql_statements("SET a ="),
        Err(ParserError::ParserError(
            "Expected variable value, found: EOF".to_string()
        )),
    );
}

#[test]
fn parse_show() {
    let stmt = verified_stmt("SHOW a a");
    assert_eq!(
        stmt,
        Statement::ShowVariable {
            variable: vec!["a".into(), "a".into()]
        }
    );

    let stmt = verified_stmt("SHOW ALL ALL");
    assert_eq!(
        stmt,
        Statement::ShowVariable {
            variable: vec!["ALL".into(), "ALL".into()]
        }
    )
}

#[test]
fn parse_deallocate() {
    let stmt = verified_stmt("DEALLOCATE a");
    assert_eq!(
        stmt,
        Statement::Deallocate {
            name: "a".into(),
            prepare: false,
        }
    );

    let stmt = verified_stmt("DEALLOCATE ALL");
    assert_eq!(
        stmt,
        Statement::Deallocate {
            name: "ALL".into(),
            prepare: false,
        }
    );

    let stmt = verified_stmt("DEALLOCATE PREPARE a");
    assert_eq!(
        stmt,
        Statement::Deallocate {
            name: "a".into(),
            prepare: true,
        }
    );

    let stmt = verified_stmt("DEALLOCATE PREPARE ALL");
    assert_eq!(
        stmt,
        Statement::Deallocate {
            name: "ALL".into(),
            prepare: true,
        }
    );
}

#[test]
fn parse_execute() {
    let stmt = verified_stmt("EXECUTE a");
    assert_eq!(
        stmt,
        Statement::Execute {
            name: "a".into(),
            parameters: vec![],
        }
    );

    let stmt = verified_stmt("EXECUTE a(1, 't')");
    assert_eq!(
        stmt,
        Statement::Execute {
            name: "a".into(),
            parameters: vec![
                Expr::Value(number("1")),
                Expr::Value(Value::SingleQuotedString("t".to_string()))
            ],
        }
    );
}

#[test]
fn parse_prepare() {
    let stmt = verified_stmt("PREPARE a AS INSERT INTO customers VALUES (a1, a2, a3)");
    let sub_stmt = match stmt {
        Statement::Prepare {
            name,
            data_types,
            statement,
            ..
        } => {
            assert_eq!(name, "a".into());
            assert!(data_types.is_empty());

            statement
        }
        _ => unreachable!(),
    };
    match sub_stmt.as_ref() {
        Statement::Insert {
            table_name,
            columns,
            source,
            ..
        } => {
            assert_eq!(table_name.to_string(), "customers");
            assert!(columns.is_empty());

            let expected_values = [vec![
                Expr::Identifier("a1".into()),
                Expr::Identifier("a2".into()),
                Expr::Identifier("a3".into()),
            ]];
            match &source.body {
                SetExpr::Values(Values(values)) => assert_eq!(values.as_slice(), &expected_values),
                _ => unreachable!(),
            }
        }
        _ => unreachable!(),
    };

    let stmt =
        verified_stmt("PREPARE a (INT, TEXT) AS SELECT * FROM customers WHERE customers.id = a1");
    let sub_stmt = match stmt {
        Statement::Prepare {
            name,
            data_types,
            statement,
            ..
        } => {
            assert_eq!(name, "a".into());
            assert_eq!(data_types, vec![DataType::Int(None), DataType::Text]);

            statement
        }
        _ => unreachable!(),
    };
    assert_eq!(
        sub_stmt,
        Box::new(Statement::Query(Box::new(verified_query(
            "SELECT * FROM customers WHERE customers.id = a1"
        ))))
    );
}

#[test]
fn parse_pg_bitwise_binary_ops() {
    let bitwise_ops = &[
        ("#", BinaryOperator::PGBitwiseXor),
        (">>", BinaryOperator::PGBitwiseShiftRight),
        ("<<", BinaryOperator::PGBitwiseShiftLeft),
    ];

    for (str_op, op) in bitwise_ops {
        let select = verified_only_select(&format!("SELECT a {} b", &str_op));
        assert_eq!(
            SelectItem::UnnamedExpr(Expr::BinaryOp {
                left: Box::new(Expr::Identifier(Ident::new("a"))),
                op: op.clone(),
                right: Box::new(Expr::Identifier(Ident::new("b"))),
            }),
            select.projection[0]
        );
    }
}

#[test]
fn parse_pg_unary_ops() {
    let pg_unary_ops = &[
        ("~", UnaryOperator::PGBitwiseNot),
        ("|/", UnaryOperator::PGSquareRoot),
        ("||/", UnaryOperator::PGCubeRoot),
        ("!!", UnaryOperator::PGPrefixFactorial),
        ("@", UnaryOperator::PGAbs),
    ];

    for (str_op, op) in pg_unary_ops {
        let select = verified_only_select(&format!("SELECT {} a", &str_op));
        assert_eq!(
            SelectItem::UnnamedExpr(Expr::UnaryOp {
                op: op.clone(),
                expr: Box::new(Expr::Identifier(Ident::new("a"))),
            }),
            select.projection[0]
        );
    }
}

#[test]
fn parse_pg_postfix_factorial() {
    let postfix_factorial = &[("!", UnaryOperator::PGPostfixFactorial)];

    for (str_op, op) in postfix_factorial {
        let select = verified_only_select(&format!("SELECT a{}", &str_op));
        assert_eq!(
            SelectItem::UnnamedExpr(Expr::UnaryOp {
                op: op.clone(),
                expr: Box::new(Expr::Identifier(Ident::new("a"))),
            }),
            select.projection[0]
        );
    }
}

#[test]
fn parse_pg_regex_match_ops() {
    let pg_regex_match_ops = &[
        ("~", BinaryOperator::PGRegexMatch),
        ("~*", BinaryOperator::PGRegexIMatch),
        ("!~", BinaryOperator::PGRegexNotMatch),
        ("!~*", BinaryOperator::PGRegexNotIMatch),
    ];

    for (str_op, op) in pg_regex_match_ops {
        let select = verified_only_select(&format!("SELECT 'abc' {} '^a'", &str_op));
        assert_eq!(
            SelectItem::UnnamedExpr(Expr::BinaryOp {
                left: Box::new(Expr::Value(Value::SingleQuotedString("abc".into()))),
                op: op.clone(),
                right: Box::new(Expr::Value(Value::SingleQuotedString("^a".into()))),
            }),
            select.projection[0]
        );
    }
}

#[test]
fn test_transaction_statement() {
    let statement = verified_stmt("SET TRANSACTION SNAPSHOT '000003A1-1'");
    assert_eq!(
        statement,
        Statement::SetTransaction {
            modes: vec![],
            snapshot: Some(Value::SingleQuotedString(String::from("000003A1-1"))),
            session: false
        }
    );
    let statement = verified_stmt("SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY, READ WRITE, ISOLATION LEVEL SERIALIZABLE");
    assert_eq!(
        statement,
        Statement::SetTransaction {
            modes: vec![
                TransactionMode::AccessMode(TransactionAccessMode::ReadOnly),
                TransactionMode::AccessMode(TransactionAccessMode::ReadWrite),
                TransactionMode::IsolationLevel(TransactionIsolationLevel::Serializable),
            ],
            snapshot: None,
            session: true
        }
    );
}

#[test]
fn parse_comments() {
    match verified_stmt("COMMENT ON COLUMN tab.name IS 'comment'") {
        Statement::Comment {
            object_type,
            object_name,
            comment: Some(comment),
        } => {
            assert_eq!("comment", comment);
            assert_eq!("tab.name", object_name.to_string());
            assert_eq!(CommentObject::Column, object_type);
        }
        _ => unreachable!(),
    }

    match verified_stmt("COMMENT ON TABLE public.tab IS 'comment'") {
        Statement::Comment {
            object_type,
            object_name,
            comment: Some(comment),
        } => {
            assert_eq!("comment", comment);
            assert_eq!("public.tab", object_name.to_string());
            assert_eq!(CommentObject::Table, object_type);
        }
        _ => unreachable!(),
    }

    match verified_stmt("COMMENT ON TABLE public.tab IS NULL") {
        Statement::Comment {
            object_type,
            object_name,
            comment: None,
        } => {
            assert_eq!("public.tab", object_name.to_string());
            assert_eq!(CommentObject::Table, object_type);
        }
        _ => unreachable!(),
    }
}
