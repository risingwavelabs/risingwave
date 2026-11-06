// Copyright 2026 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::sink::{Result, SinkError};

fn parse_error(message: impl std::fmt::Display) -> SinkError {
    SinkError::ClickHouse(format!("Cannot parse Distributed table target: {message}"))
}

/// Extracts the underlying database and table from `system.tables.engine_full`.
///
/// Accepts identifiers and quoted names, plus `currentDatabase()` for the database.
/// An empty database string also uses `current_database`, which must be the effective
/// database of the `ClickHouse` session.
pub(super) fn parse_distributed_table(
    engine: &str,
    current_database: &str,
) -> Result<(String, String)> {
    let args = distributed_arguments(engine)?;
    if !(3..=5).contains(&args.len()) {
        return Err(parse_error(format!(
            "Distributed requires 3 to 5 arguments, but received {}. Expected syntax: Distributed(cluster, database, table[, sharding_key[, policy_name]])",
            args.len()
        )));
    }
    if let Some(index) = args.iter().position(|arg| arg.is_empty()) {
        return Err(parse_error(format!(
            "argument {} must not be empty",
            index + 1
        )));
    }

    let is_current_database = args[1]
        .strip_prefix("currentDatabase")
        .and_then(|rest| rest.trim_start().strip_prefix('('))
        .and_then(|rest| rest.strip_suffix(')'))
        .is_some_and(|args| args.trim().is_empty());
    let mut database = if is_current_database {
        current_database.to_owned()
    } else {
        parse_name(args[1], "database")?
    };
    if database.is_empty() {
        database = current_database.to_owned();
    }
    let table = parse_name(args[2], "table")?;
    if database.is_empty() || table.is_empty() {
        return Err(parse_error("database and table must not be empty"));
    }
    Ok((database, table))
}

fn distributed_arguments(engine: &str) -> Result<Vec<&str>> {
    let input = engine
        .trim()
        .strip_prefix("Distributed")
        .and_then(|rest| rest.trim_start().strip_prefix('('))
        .ok_or_else(|| parse_error("expected Distributed(...) engine definition"))?;
    let bytes = input.as_bytes();
    let mut args = Vec::new();
    let mut brackets = Vec::new();
    let mut start = 0;
    let mut pos = 0;
    while pos < bytes.len() {
        match bytes[pos] {
            b'\'' | b'"' | b'`' => {
                let (_, len) = read_quoted(&bytes[pos..])?;
                pos += len;
                continue;
            }
            b'(' | b'[' => brackets.push(bytes[pos]),
            b')' if brackets.is_empty() => {
                args.push(input[start..pos].trim());
                let suffix = input[pos + 1..].trim();
                // Allow a trailing SETTINGS clause in `engine``; it does not
                // affect the underlying database or table name.
                if !suffix.is_empty()
                    && !suffix.strip_prefix("SETTINGS").is_some_and(|rest| {
                        rest.starts_with(char::is_whitespace) && !rest.trim().is_empty()
                    })
                {
                    return Err(parse_error("unexpected text after Distributed(...)"));
                }
                return Ok(args);
            }
            b')' | b']' => {
                let expected = if bytes[pos] == b')' { b'(' } else { b'[' };
                if brackets.pop() != Some(expected) {
                    return Err(parse_error("unbalanced parentheses or brackets"));
                }
            }
            b',' if brackets.is_empty() => {
                args.push(input[start..pos].trim());
                start = pos + 1;
            }
            _ => {}
        }
        pos += 1;
    }
    Err(parse_error("missing closing parenthesis"))
}

fn parse_name(arg: &str, kind: &str) -> Result<String> {
    let bytes = arg.as_bytes();
    if matches!(bytes.first(), Some(b'\'' | b'"' | b'`')) {
        let (name, len) = read_quoted(bytes)?;
        if len == bytes.len() {
            return String::from_utf8(name)
                .map_err(|_| parse_error(format!("{kind} name is not valid UTF-8")));
        }
    } else if bytes
        .first()
        .is_some_and(|c| c.is_ascii_alphabetic() || *c == b'_')
        && bytes
            .iter()
            .all(|c| c.is_ascii_alphanumeric() || *c == b'_')
    {
        return Ok(arg.to_owned());
    }

    Err(parse_error(format!("unsupported {kind} argument {arg:?}")))
}

/// Reads a `ClickHouse` quoted string or identifier, including its delimiters.
fn read_quoted(input: &[u8]) -> Result<(Vec<u8>, usize)> {
    let quote = input[0];
    let mut value = Vec::new();
    let mut pos = 1;
    while pos < input.len() {
        let byte = input[pos];
        pos += 1;
        if byte == quote {
            if input.get(pos) != Some(&quote) {
                return Ok((value, pos));
            }
            pos += 1;
            value.push(quote);
        } else if byte == b'\\' {
            let escaped = *input
                .get(pos)
                .ok_or_else(|| parse_error("unterminated escape sequence"))?;
            pos += 1;
            match escaped {
                b'x' => {
                    let hex = input
                        .get(pos..pos + 2)
                        .filter(|hex| hex.iter().all(u8::is_ascii_hexdigit))
                        .ok_or_else(|| parse_error("expected two hex digits after \\x"))?;
                    value.push(
                        (char::from(hex[0]).to_digit(16).unwrap() * 16
                            + char::from(hex[1]).to_digit(16).unwrap())
                            as u8,
                    );
                    pos += 2;
                }
                b'N' => {}
                b'a' => value.push(7),
                b'b' => value.push(8),
                b'e' => value.push(27),
                b'f' => value.push(12),
                b'n' => value.push(b'\n'),
                b'r' => value.push(b'\r'),
                b't' => value.push(b'\t'),
                b'v' => value.push(11),
                b'0' => value.push(0),
                b'\\' | b'\'' | b'"' | b'`' | b'/' | b'=' | 0..=31 => value.push(escaped),
                _ => value.extend_from_slice(&[b'\\', escaped]),
            }
        } else {
            value.push(byte);
        }
    }
    Err(parse_error("unterminated quoted string or identifier"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_distributed_table_names() {
        for (engine, database, table) in [
            ("Distributed(logs, analytics, hits)", "analytics", "hits"),
            (
                "Distributed('logs', 'analytics', 'hits', rand())",
                "analytics",
                "hits",
            ),
            (
                "Distributed(logs, `other-db`, \"events.local\", id, 'hot')",
                "other-db",
                "events.local",
            ),
            (
                "Distributed(logs, 'db(a,b)', 'events,)(')",
                "db(a,b)",
                "events,)(",
            ),
            ("Distributed(logs, _db1, _table2)", "_db1", "_table2"),
            (
                "Distributed(logs, currentDatabase, hits)",
                "currentDatabase",
                "hits",
            ),
            (
                "Distributed(logs, 'currentDatabase()', hits)",
                "currentDatabase()",
                "hits",
            ),
            (
                "Distributed(logs, 'currentDatabase ( )', hits)",
                "currentDatabase ( )",
                "hits",
            ),
            ("Distributed(logs, ' db ', ' table ')", " db ", " table "),
        ] {
            assert_eq!(
                parse_distributed_table(engine, "default").unwrap(),
                (database.to_owned(), table.to_owned()),
                "{engine}"
            );
        }
    }

    #[test]
    fn test_distributed_current_database_and_nested_arguments() {
        for engine in [
            "Distributed(logs, currentDatabase(), hits, rand())",
            "Distributed(logs, currentDatabase (), hits)",
            "Distributed(logs, currentDatabase( ), hits)",
            "Distributed(logs, currentDatabase (  ), hits)",
            "Distributed(logs, currentDatabase\t(\n\t), hits)",
            "Distributed('logs(a,b)', currentDatabase(), hits, cityHash64(a, b))",
            " Distributed ( logs , currentDatabase() , hits , cityHash64(a, b) ) ",
            "Distributed(logs, '', hits, cityHash64(tuple(a, b), [1, 2]))",
            "Distributed(logs, currentDatabase(), hits, cityHash64('a,)(', b))",
            "Distributed(logs, currentDatabase(), hits, cityHash64(\"a,b\", `c)d`))",
            r"Distributed(logs, currentDatabase(), hits, cityHash64('a\'),b', '\xFF'))",
            "Distributed(logs, currentDatabase(), hits, id, 'hot') SETTINGS fsync_after_insert = 1, fsync_directories = 1",
        ] {
            assert_eq!(
                parse_distributed_table(engine, "session_db").unwrap(),
                ("session_db".to_owned(), "hits".to_owned()),
                "{engine}"
            );
        }
        assert!(parse_distributed_table("Distributed(logs, '', hits)", "").is_err());
        assert!(parse_distributed_table("Distributed(logs, currentDatabase(), hits)", "").is_err());
    }

    #[test]
    fn test_distributed_quoted_escapes() {
        for (engine, database, table) in [
            (
                r"Distributed(logs, 'db\'name', 'table\\name')",
                "db'name",
                r"table\name",
            ),
            (
                r"Distributed(logs, `db\`name`, `table``name`)",
                "db`name",
                "table`name",
            ),
            (
                r#"Distributed(logs, "db\"name", "table""name")"#,
                "db\"name",
                "table\"name",
            ),
            (
                "Distributed(logs, 'db''name', 'table''name')",
                "db'name",
                "table'name",
            ),
            (
                r"Distributed(logs, 'db\x2cname', 't\xC3\xA9')",
                "db,name",
                "té",
            ),
            (
                r"Distributed(logs, 'db\nname', 't\tname')",
                "db\nname",
                "t\tname",
            ),
            (
                r"Distributed(logs, 'db\qname', 't\Nname')",
                r"db\qname",
                "tname",
            ),
        ] {
            assert_eq!(
                parse_distributed_table(engine, "default").unwrap(),
                (database.to_owned(), table.to_owned()),
                "{engine}"
            );
        }
    }

    #[test]
    fn test_distributed_invalid_definitions() {
        for engine in [
            "MergeTree()",
            "DistributedExtra(logs, db, t)",
            "CREATE TABLE t ENGINE = Distributed(logs, db, t)",
            "Distributed()",
            "Distributed(logs, db)",
            "Distributed(, db, t)",
            "Distributed(logs, , t)",
            "Distributed(logs, db, )",
            "Distributed(logs, db, t,)",
            "Distributed(logs, db, t, id, 'hot', extra)",
            "Distributed(logs, db, t",
            "Distributed(logs, db, t, cityHash64(a, b)",
            "Distributed(logs, db, t, cityHash64([1, 2)))",
            "Distributed(logs, db, t])",
            "Distributed(logs, 'db, t)",
            "Distributed(logs, db, `t)",
            r"Distributed(logs, db, 't\",
            r"Distributed(logs, 'db\xQ1', t)",
            r"Distributed(logs, db, '\xFF')",
            "Distributed(logs, db, '')",
            "Distributed(logs, db, t))",
            "Distributed(logs, db, t) garbage",
            "Distributed(logs, db, t) SETTINGS",
            "Distributed(logs, concat('d', 'b'), t)",
            "Distributed(logs, currentDatabase('db'), t)",
            "Distributed(logs, current Database(), t)",
            "Distributed(logs, 'db' || 'other', t)",
            "Distributed(logs, db, concat('t', '1'))",
            "Distributed(logs, db, db.table)",
            "Distributed(logs, db, 123)",
        ] {
            assert!(
                parse_distributed_table(engine, "default").is_err(),
                "{engine}"
            );
        }
    }
}
