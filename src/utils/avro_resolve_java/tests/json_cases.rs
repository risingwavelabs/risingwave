// ```
// cargo llvm-cov --html --ignore-run-fail
// cargo llvm-cov --html --ignore-run-fail --test json_cases -- happy.json
// ```

use std::path::Path;

use serde::Deserialize;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    hex: Vec<String>,
    writer: serde_json::Value,
    reader: serde_json::Value,
}

// datatest-stable requires the Result signature; panics/asserts still work,
// so the trailing Ok(()) is the only concession to it. The String parameter
// makes the harness read the file itself.
fn case(_path: &Path, raw: String) -> datatest_stable::Result<()> {
    let case: Case = serde_json::from_str(&raw).unwrap();
    let bytes = hex::decode(case.hex.concat()).unwrap();
    let writer = apache_avro::Schema::parse(&case.writer).unwrap();
    let reader = apache_avro::Schema::parse(&case.reader).unwrap();
    let res_jv = avro_resolve_java::from_avro_datum(&writer, &mut bytes.as_slice(), Some(&reader));
    // panic!("{res_jv:#?}");
    #[expect(clippy::disallowed_methods)]
    let res_rs = apache_avro::from_avro_datum(&writer, &mut bytes.as_slice(), Some(&reader));
    assert_eq!(res_jv.ok(), res_rs.ok());
    Ok(())
}

datatest_stable::harness! {
    { test = case, root = "tests/cases", pattern = r"\.json$" },
}
