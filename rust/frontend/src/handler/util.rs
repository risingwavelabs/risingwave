use itertools::Itertools;
use risingwave_common::array::DataChunk;
use pgwire::types::Row;

fn to_pg_rows(chunk: DataChunk) -> Vec<Row> {
    chunk.rows()
        .map(|r| {
            Row::new(
                r.0.into_iter()
                    .map(|data| data.map(|d| d.to_string()))
                    .collect_vec(),
            )
        })
        .collect_vec()
}

#[test]
fn test_to_pg_rows() {
    use risingwave_common::array::I32Array;
    use risingwave_common::array::I64Array;
    use risingwave_common::array::F32Array;
    use risingwave_common::array::Utf8Array;
    let chunk = DataChunk::new(
        vec![
            column_nonnull!(I32Array, [1, 2, 3, 4]),
            column!(I64Array, [Some(6), None, Some(7), None]),
            column!(F32Array, [Some(6.01), None, Some(7.01), None]),
            column!(Utf8Array, [Some("aaa"), None, Some("vvv"), None]),
        ],
        None,
    );
    let rows = to_pg_rows(chunk);
    let expected = vec![
        vec![
            Some("1".to_string()),
            Some("6".to_string()),
            Some("6.01".to_string()),
            Some("aaa".to_string()),
        ],
        vec![Some("2".to_string()), None, None, None],
        vec![
            Some("3".to_string()),
            Some("7".to_string()),
            Some("7.01".to_string()),
            Some("vvv".to_string()),
        ],
        vec![Some("4".to_string()), None, None, None],
    ];
    let vec = rows
        .into_iter()
        .map(|r| {
            r.values()
                .iter()
                .map(|s| {
                    s.map(|t| t.to_string())
                })
                .collect_vec()
        })
        .collect_vec();

    assert_eq!(vec, expected);
}