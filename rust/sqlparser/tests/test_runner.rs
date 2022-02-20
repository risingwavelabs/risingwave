// Data-driven tests.

use itertools::Itertools;
use risingwave_sqlparser::parser::Parser;

// 1. The input sql.
// 2. ---
// 3. If sql parsing succeeds, the line is the formatted sql.
//    Otherwise, it is the error message.
// 4. => No exist if the parsing is expected to fail.
// 5. The formatted ast.
#[derive(PartialEq, Eq, Debug)]
struct TestCase {
    input: String,
    formatted_sql: Option<String>,
    error_msg: Option<String>,
    formatted_ast: Option<String>,
}

fn parse_test_case(case_str: &str) -> TestCase {
    let bulks: Vec<String> = case_str
        .lines()
        .collect_vec()
        .split(|l| *l == "---")
        .map(|slice| slice.join("\n"))
        .collect();
    assert_eq!(bulks.len(), 2);
    let input = bulks[0].to_string();
    let remain = bulks[1].to_string();
    let bulks: Vec<String> = remain
        .lines()
        .collect_vec()
        .split(|l| *l == "=>")
        .map(|slice| slice.join("\n"))
        .collect();
    if bulks.len() == 1 {
        TestCase {
            input,
            error_msg: Some(bulks[0].to_string()),
            formatted_sql: None,
            formatted_ast: None,
        }
    } else {
        assert_eq!(bulks.len(), 2);
        TestCase {
            input,
            error_msg: None,
            formatted_sql: Some(bulks[0].to_string()),
            formatted_ast: Some(bulks[1].to_string()),
        }
    }
}

fn run_test_case(case_str: &str) {
    let c = parse_test_case(case_str);

    let result = Parser::parse_sql(&c.input);
    if let Some(error_msg) = c.error_msg {
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().to_string(), error_msg);
    } else {
        assert!(result.is_ok());
        let ast = &result.unwrap()[0];
        let formatted_sql = format!("{}", ast);
        assert_eq!(c.formatted_sql, Some(formatted_sql));
        let formatted_ast = format!("{:?}", ast);
        assert_eq!(c.formatted_ast, Some(formatted_ast));
    }
}

fn run_test_file(_file_name: &str, file_content: &str) {
    let file_content = remove_comments(file_content);
    let cases = split_test_cases(&file_content);
    for case in cases {
        run_test_case(&case);
    }
}

/// Remove the '#' commented parts.
fn remove_comments(file_content: &str) -> String {
    let mut lines = file_content
        .lines()
        .map(|l| l.trim())
        .filter(|line| !line.starts_with("#"))
        .map(|s| s.to_string())
        .collect::<Vec<String>>();
    lines.iter_mut().for_each(|line| {
        if let Some(pos) = line.find('#') {
            line.truncate(pos);
        }
    });
    lines.join("\n")
}

// The test-cases are separated by a blank line.
fn split_test_cases(file_content: &str) -> Vec<String> {
    file_content
        .lines()
        .collect_vec()
        .split(|l| l.is_empty())
        .map(|slice| slice.join("\n"))
        .filter(|case| !case.is_empty())
        .collect_vec()
}

// Traverses the 'testdata/' directory and runs all files.
#[test]
fn run_all_test_files() {
    use walkdir::WalkDir;
    for entry in WalkDir::new("./tests/testdata/") {
        let entry = entry.unwrap();
        if !entry.path().is_file() {
            continue;
        }
        let file_content = std::fs::read_to_string(entry.path()).unwrap();
        run_test_file(entry.path().to_str().unwrap(), &file_content);
    }
}

#[test]
fn test_remove_comments() {
    assert_eq!(
        remove_comments("### line 1\nline 2###comments\nline 3"),
        "line 2\nline 3"
    );
}

#[test]
fn test_split_test_cases() {
    assert_eq!(
        split_test_cases("line 1\n   \n  \nline 2\nline 3"),
        vec!["line 1".to_string(), "line 2\nline 3".to_string(),]
    );
}

#[test]
fn test_parse_test_case() {
    assert_eq!(
        parse_test_case("abc\n---\nsyntax error"),
        TestCase {
            input: "abc".to_string(),
            formatted_sql: None,
            error_msg: Some("syntax error".to_string()),
            formatted_ast: None
        }
    );
    assert_eq!(
        parse_test_case("abc\n---\nabc\n=>\nSql { abc }"),
        TestCase {
            input: "abc".to_string(),
            formatted_sql: Some("abc".to_string()),
            error_msg: None,
            formatted_ast: Some("Sql { abc }".to_string()),
        }
    );
}
