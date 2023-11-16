// Copyright 2023 RisingWave Labs
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

// std try_collect is slower than itertools
// #![feature(iterator_try_collect)]

// allow using `zip`.
// `zip_eq` is a source of poor performance.
#![allow(clippy::disallowed_methods)]

risingwave_expr_impl::enable!();

use criterion::async_executor::FuturesExecutor;
use criterion::{criterion_group, criterion_main, Criterion};
use risingwave_common::array::*;
use risingwave_common::types::test_utils::IntervalTestExt;
use risingwave_common::types::*;
use risingwave_expr::aggregate::{build_append_only, AggArgs, AggCall, AggKind};
use risingwave_expr::expr::*;
use risingwave_expr::sig::FUNCTION_REGISTRY;
use risingwave_expr::ExprError;
use risingwave_pb::expr::expr_node::PbType;

criterion_group!(benches, bench_expr, bench_raw);
criterion_main!(benches);

const CHUNK_SIZE: usize = 1024;

fn bench_expr(c: &mut Criterion) {
    use itertools::Itertools;

    let input = StreamChunk::from(DataChunk::new(
        vec![
            BoolArray::from_iter((1..=CHUNK_SIZE).map(|i| i % 2 == 0)).into_ref(),
            I16Array::from_iter((1..=CHUNK_SIZE).map(|_| 1)).into_ref(),
            I32Array::from_iter((1..=CHUNK_SIZE).map(|_| 1)).into_ref(),
            I64Array::from_iter((1..=CHUNK_SIZE).map(|_| 1)).into_ref(),
            F32Array::from_iter((1..=CHUNK_SIZE).map(|i| i as f32)).into_ref(),
            F64Array::from_iter((1..=CHUNK_SIZE).map(|i| i as f64)).into_ref(),
            DecimalArray::from_iter((1..=CHUNK_SIZE).map(Decimal::from)).into_ref(),
            DateArray::from_iter((1..=CHUNK_SIZE).map(|_| Date::default())).into_ref(),
            TimeArray::from_iter((1..=CHUNK_SIZE).map(|_| Time::default())).into_ref(),
            TimestampArray::from_iter((1..=CHUNK_SIZE).map(|_| Timestamp::default())).into_ref(),
            TimestamptzArray::from_iter((1..=CHUNK_SIZE).map(|_| Timestamptz::default()))
                .into_ref(),
            IntervalArray::from_iter((1..=CHUNK_SIZE).map(|i| Interval::from_days(i as _)))
                .into_ref(),
            Utf8Array::from_iter_display((1..=CHUNK_SIZE).map(Some)).into_ref(),
            Utf8Array::from_iter_display((1..=CHUNK_SIZE).map(Some))
                .into_bytes_array()
                .into_ref(),
            // special varchar arrays
            // 14: timezone
            Utf8Array::from_iter_display((1..=CHUNK_SIZE).map(|_| Some("Australia/Sydney")))
                .into_ref(),
            // 15: time field
            Utf8Array::from_iter_display(
                [
                    "microseconds",
                    "milliseconds",
                    "second",
                    "minute",
                    "hour",
                    "day",
                    // "week",
                    "month",
                    "quarter",
                    "year",
                    "decade",
                    "century",
                    "millennium",
                ]
                .into_iter()
                .cycle()
                .take(CHUNK_SIZE)
                .map(Some),
            )
            .into_ref(),
            // 16: extract field for date
            Utf8Array::from_iter_display(
                [
                    "DAY",
                    "MONTH",
                    "YEAR",
                    "DOW",
                    "DOY",
                    "MILLENNIUM",
                    "CENTURY",
                    "DECADE",
                    "ISOYEAR",
                    "QUARTER",
                    "WEEK",
                    "ISODOW",
                    "EPOCH",
                    "JULIAN",
                ]
                .into_iter()
                .cycle()
                .take(CHUNK_SIZE)
                .map(Some),
            )
            .into_ref(),
            // 17: extract field for time
            Utf8Array::from_iter_display(
                [
                    "Hour",
                    "Minute",
                    "Second",
                    "Millisecond",
                    "Microsecond",
                    "Epoch",
                ]
                .into_iter()
                .cycle()
                .take(CHUNK_SIZE)
                .map(Some),
            )
            .into_ref(),
            // 18: extract field for timestamptz
            Utf8Array::from_iter_display(["EPOCH"].into_iter().cycle().take(CHUNK_SIZE).map(Some))
                .into_ref(),
            // 19: boolean string
            Utf8Array::from_iter_display([Some(true)].into_iter().cycle().take(CHUNK_SIZE))
                .into_ref(),
            // 20: date string
            Utf8Array::from_iter_display(
                [Some(Date::default())].into_iter().cycle().take(CHUNK_SIZE),
            )
            .into_ref(),
            // 21: time string
            Utf8Array::from_iter_display(
                [Some(Time::default())].into_iter().cycle().take(CHUNK_SIZE),
            )
            .into_ref(),
            // 22: timestamp string
            Utf8Array::from_iter_display(
                [Some(Timestamp::default())]
                    .into_iter()
                    .cycle()
                    .take(CHUNK_SIZE),
            )
            .into_ref(),
            // 23: timestamptz string
            Utf8Array::from_iter_display(
                [Some("2021-04-01 00:00:00+00:00")]
                    .into_iter()
                    .cycle()
                    .take(CHUNK_SIZE),
            )
            .into_ref(),
            // 24: interval string
            Utf8Array::from_iter_display(
                [Some(Interval::default())]
                    .into_iter()
                    .cycle()
                    .take(CHUNK_SIZE),
            )
            .into_ref(),
            // 25: serial array
            SerialArray::from_iter((1..=CHUNK_SIZE).map(|i| Serial::from(i as i64))).into_ref(),
            // 26: jsonb array
            JsonbArray::from_iter((1..=CHUNK_SIZE).map(|i| JsonbVal::from(i as f64))).into_ref(),
            // 27: int256 array
            Int256Array::from_iter((1..=CHUNK_SIZE).map(|_| Int256::from(1))).into_ref(),
            // 28: extract field for interval
            Utf8Array::from_iter_display(
                [
                    "Millennium",
                    "Century",
                    "Decade",
                    "Year",
                    "Month",
                    "Day",
                    "Hour",
                    "Minute",
                    "Second",
                    "Millisecond",
                    "Microsecond",
                    "Epoch",
                ]
                .into_iter()
                .cycle()
                .take(CHUNK_SIZE)
                .map(Some),
            )
            .into_ref(),
            // 29: timestamp string for to_timestamp
            Utf8Array::from_iter_display(
                [Some("2021/04/01 00:00:00")]
                    .into_iter()
                    .cycle()
                    .take(CHUNK_SIZE),
            )
            .into_ref(),
        ],
        CHUNK_SIZE,
    ));
    let inputrefs = [
        InputRefExpression::new(DataType::Boolean, 0),
        InputRefExpression::new(DataType::Int16, 1),
        InputRefExpression::new(DataType::Int32, 2),
        InputRefExpression::new(DataType::Int64, 3),
        InputRefExpression::new(DataType::Serial, 25),
        InputRefExpression::new(DataType::Float32, 4),
        InputRefExpression::new(DataType::Float64, 5),
        InputRefExpression::new(DataType::Decimal, 6),
        InputRefExpression::new(DataType::Date, 7),
        InputRefExpression::new(DataType::Time, 8),
        InputRefExpression::new(DataType::Timestamp, 9),
        InputRefExpression::new(DataType::Timestamptz, 10),
        InputRefExpression::new(DataType::Interval, 11),
        InputRefExpression::new(DataType::Varchar, 12),
        InputRefExpression::new(DataType::Bytea, 13),
        InputRefExpression::new(DataType::Jsonb, 26),
        InputRefExpression::new(DataType::Int256, 27),
    ];
    let input_index_for_type = |ty: &DataType| {
        inputrefs
            .iter()
            .find(|r| &r.return_type() == ty)
            .unwrap_or_else(|| panic!("expression not found for {ty:?}"))
            .index()
    };
    const TIMEZONE: usize = 14;
    const TIME_FIELD: usize = 15;
    const EXTRACT_FIELD_DATE: usize = 16;
    const EXTRACT_FIELD_TIME: usize = 17;
    const EXTRACT_FIELD_TIMESTAMP: usize = 16;
    const EXTRACT_FIELD_TIMESTAMPTZ: usize = 18;
    const EXTRACT_FIELD_INTERVAL: usize = 28;
    const BOOL_STRING: usize = 19;
    const NUMBER_STRING: usize = 12;
    const DATE_STRING: usize = 20;
    const TIME_STRING: usize = 21;
    const TIMESTAMP_STRING: usize = 22;
    const TIMESTAMPTZ_STRING: usize = 23;
    const INTERVAL_STRING: usize = 24;
    const TIMESTAMP_FORMATTED_STRING: usize = 29;

    c.bench_function("inputref", |bencher| {
        let inputref = inputrefs[0].clone().boxed();
        bencher
            .to_async(FuturesExecutor)
            .iter(|| inputref.eval(&input))
    });
    c.bench_function("constant", |bencher| {
        let constant = LiteralExpression::new(DataType::Int32, Some(1_i32.into()));
        bencher
            .to_async(FuturesExecutor)
            .iter(|| constant.eval(&input))
    });
    c.bench_function("extract(constant)", |bencher| {
        let extract = build_from_pretty(format!(
            "(extract:decimal HOUR:varchar ${}:timestamp)",
            input_index_for_type(&DataType::Timestamp)
        ));
        bencher
            .to_async(FuturesExecutor)
            .iter(|| extract.eval(&input))
    });

    let sigs = FUNCTION_REGISTRY
        .iter_scalars()
        .sorted_by_cached_key(|sig| format!("{sig:?}"));
    'sig: for sig in sigs {
        if (sig.inputs_type.iter())
            .chain([&sig.ret_type])
            .any(|t| !t.is_exact() || t.as_exact().is_array())
        {
            // TODO: support struct and array
            println!("todo: {sig:?}");
            continue;
        }
        if [
            "date_trunc(character varying, timestamp with time zone) -> timestamp with time zone",
            "to_timestamp1(character varying, character varying) -> timestamp with time zone",
            "to_char(timestamp with time zone, character varying) -> character varying",
        ]
        .contains(&format!("{sig:?}").as_str())
        {
            println!("ignore: {sig:?}");
            continue;
        }

        fn string_literal(s: &str) -> BoxedExpression {
            LiteralExpression::new(DataType::Varchar, Some(s.into())).boxed()
        }

        let mut children = vec![];
        for (i, t) in sig.inputs_type.iter().enumerate() {
            use DataType::*;
            let idx = match (sig.name.as_scalar(), i) {
                (PbType::ToTimestamp1, 0) => TIMESTAMP_FORMATTED_STRING,
                (PbType::ToChar | PbType::ToTimestamp1, 1) => {
                    children.push(string_literal("YYYY/MM/DD HH:MM:SS"));
                    continue;
                }
                (PbType::ToChar | PbType::ToTimestamp1, 2) => {
                    children.push(string_literal("Australia/Sydney"));
                    continue;
                }
                (PbType::IsJson, 1) => {
                    children.push(string_literal("VALUE"));
                    continue;
                }
                (PbType::Cast, 0) if t.as_exact() == &Varchar => match sig.ret_type.as_exact() {
                    Boolean => BOOL_STRING,
                    Int16 | Int32 | Int64 | Float32 | Float64 | Decimal => NUMBER_STRING,
                    Date => DATE_STRING,
                    Time => TIME_STRING,
                    Timestamp => TIMESTAMP_STRING,
                    Timestamptz => TIMESTAMPTZ_STRING,
                    Interval => INTERVAL_STRING,
                    Bytea => NUMBER_STRING, // any
                    _ => {
                        println!("todo: {sig:?}");
                        continue 'sig;
                    }
                },
                (PbType::AtTimeZone, 1) => TIMEZONE,
                (PbType::DateTrunc, 0) => TIME_FIELD,
                (PbType::DateTrunc, 2) => TIMEZONE,
                (PbType::Extract, 0) => match sig.inputs_type[1].as_exact() {
                    Date => EXTRACT_FIELD_DATE,
                    Time => EXTRACT_FIELD_TIME,
                    Timestamp => EXTRACT_FIELD_TIMESTAMP,
                    Timestamptz => EXTRACT_FIELD_TIMESTAMPTZ,
                    Interval => EXTRACT_FIELD_INTERVAL,
                    t => panic!("unexpected type: {t:?}"),
                },
                _ => input_index_for_type(t.as_exact()),
            };
            children.push(InputRefExpression::new(t.as_exact().clone(), idx).boxed());
        }
        let expr = build_func(
            sig.name.as_scalar(),
            sig.ret_type.as_exact().clone(),
            children,
        )
        .unwrap();
        c.bench_function(&format!("{sig:?}"), |bencher| {
            bencher.to_async(FuturesExecutor).iter(|| expr.eval(&input))
        });
    }

    let sigs = FUNCTION_REGISTRY
        .iter_aggregates()
        .sorted_by_cached_key(|sig| format!("{sig:?}"));
    for sig in sigs {
        if matches!(
            sig.name.as_aggregate(),
            AggKind::PercentileDisc | AggKind::PercentileCont
        ) || (sig.inputs_type.iter())
            .chain([&sig.ret_type])
            .any(|t| !t.is_exact())
        {
            println!("todo: {sig:?}");
            continue;
        }
        let agg = match build_append_only(&AggCall {
            kind: sig.name.as_aggregate(),
            args: match sig.inputs_type.as_slice() {
                [] => AggArgs::None,
                [t] => AggArgs::Unary(t.as_exact().clone(), input_index_for_type(t.as_exact())),
                [t1, t2] => AggArgs::Binary(
                    [t1.as_exact().clone(), t2.as_exact().clone()],
                    [
                        input_index_for_type(t1.as_exact()),
                        input_index_for_type(t2.as_exact()),
                    ],
                ),
                _ => {
                    println!("todo: {sig:?}");
                    continue;
                }
            },
            return_type: sig.ret_type.as_exact().clone(),
            column_orders: vec![],
            filter: None,
            distinct: false,
            direct_args: vec![],
        }) {
            Ok(agg) => agg,
            Err(e) => {
                println!("error: {e}");
                continue;
            }
        };
        let input = match sig.inputs_type.as_slice() {
            [] => input.project(&[]),
            [t] => input.project(&[input_index_for_type(t.as_exact())]),
            [t1, t2] => input.project(&[
                input_index_for_type(t1.as_exact()),
                input_index_for_type(t2.as_exact()),
            ]),
            _ => unreachable!(),
        };
        c.bench_function(&format!("{sig:?}"), |bencher| {
            bencher
                .to_async(FuturesExecutor)
                .iter(|| async { agg.update(&mut agg.create_state(), &input).await.unwrap() })
        });
    }
}

/// Evaluate on raw Rust array.
///
/// This could be used as a baseline to compare and tune our expressions.
fn bench_raw(c: &mut Criterion) {
    // ~55ns
    c.bench_function("raw/sum/i32", |bencher| {
        let a = (0..CHUNK_SIZE as i32).collect::<Vec<_>>();
        bencher.iter(|| a.iter().sum::<i32>())
    });
    // ~90ns
    c.bench_function("raw/add/i32", |bencher| {
        let a = (0..CHUNK_SIZE as i32).collect::<Vec<_>>();
        let b = (0..CHUNK_SIZE as i32).collect::<Vec<_>>();
        bencher.iter(|| {
            a.iter()
                .zip(b.iter())
                .map(|(a, b)| a + b)
                .collect::<Vec<_>>()
        })
    });
    // ~600ns
    c.bench_function("raw/add/i32/zip_eq", |bencher| {
        let a = (0..CHUNK_SIZE as i32).collect::<Vec<_>>();
        let b = (0..CHUNK_SIZE as i32).collect::<Vec<_>>();
        bencher.iter(|| {
            itertools::Itertools::zip_eq(a.iter(), b.iter())
                .map(|(a, b)| a + b)
                .collect::<Vec<_>>()
        })
    });
    // ~950ns
    c.bench_function("raw/add/Option<i32>/zip_eq", |bencher| {
        let a = (0..CHUNK_SIZE as i32).map(Some).collect::<Vec<_>>();
        let b = (0..CHUNK_SIZE as i32).map(Some).collect::<Vec<_>>();
        bencher.iter(|| {
            itertools::Itertools::zip_eq(a.iter(), b.iter())
                .map(|(a, b)| match (a, b) {
                    (Some(a), Some(b)) => Some(a + b),
                    _ => None,
                })
                .collect::<Vec<_>>()
        })
    });
    enum Error {
        Overflow,
        Cast,
    }
    // ~2100ns
    c.bench_function("raw/add/Option<i32>/zip_eq,checked", |bencher| {
        let a = (0..CHUNK_SIZE as i32).map(Some).collect::<Vec<_>>();
        let b = (0..CHUNK_SIZE as i32).map(Some).collect::<Vec<_>>();
        bencher.iter(|| {
            use itertools::Itertools;
            itertools::Itertools::zip_eq(a.iter(), b.iter())
                .map(|(a, b)| match (a, b) {
                    (Some(a), Some(b)) => a.checked_add(*b).ok_or(Error::Overflow).map(Some),
                    _ => Ok(None),
                })
                .try_collect::<_, Vec<_>, Error>()
        })
    });
    // ~2400ns
    c.bench_function("raw/add/Option<i32>/zip_eq,checked,cast", |bencher| {
        let a = (0..CHUNK_SIZE as i32).map(Some).collect::<Vec<_>>();
        let b = (0..CHUNK_SIZE as i32).map(Some).collect::<Vec<_>>();
        #[allow(clippy::useless_conversion)]
        fn checked_add(a: i32, b: i32) -> std::result::Result<i32, Error> {
            let a: i32 = a.try_into().map_err(|_| Error::Cast)?;
            let b: i32 = b.try_into().map_err(|_| Error::Cast)?;
            a.checked_add(b).ok_or(Error::Overflow)
        }
        bencher.iter(|| {
            use itertools::Itertools;
            itertools::Itertools::zip_eq(a.iter(), b.iter())
                .map(|(a, b)| match (a, b) {
                    (Some(a), Some(b)) => checked_add(*a, *b).map(Some),
                    _ => Ok(None),
                })
                .try_collect::<_, Vec<_>, Error>()
        })
    });
    // ~3100ns
    c.bench_function(
        "raw/add/Option<i32>/zip_eq,checked,cast,collect_array",
        |bencher| {
            let a = (0..CHUNK_SIZE as i32).map(Some).collect::<Vec<_>>();
            let b = (0..CHUNK_SIZE as i32).map(Some).collect::<Vec<_>>();
            #[allow(clippy::useless_conversion)]
            fn checked_add(a: i32, b: i32) -> std::result::Result<i32, Error> {
                let a: i32 = a.try_into().map_err(|_| Error::Cast)?;
                let b: i32 = b.try_into().map_err(|_| Error::Cast)?;
                a.checked_add(b).ok_or(Error::Overflow)
            }
            bencher.iter(|| {
                use itertools::Itertools;
                itertools::Itertools::zip_eq(a.iter(), b.iter())
                    .map(|(a, b)| match (a, b) {
                        (Some(a), Some(b)) => checked_add(*a, *b).map(Some),
                        _ => Ok(None),
                    })
                    .try_collect::<_, I32Array, Error>()
            })
        },
    );

    // ~360ns
    // This should be the optimization goal for our add expression.
    c.bench_function("TBD/add(int32,int32)", |bencher| {
        bencher.iter(|| {
            let a = (0..CHUNK_SIZE as i32).collect::<I32Array>();
            let b = (0..CHUNK_SIZE as i32).collect::<I32Array>();
            assert_eq!(a.len(), b.len());
            let mut c = (a.raw_iter())
                .zip(b.raw_iter())
                .map(|(a, b)| a + b)
                .collect::<I32Array>();
            let mut overflow = false;
            for ((a, b), c) in a.raw_iter().zip(b.raw_iter()).zip(c.raw_iter()) {
                overflow |= (c ^ a) & (c ^ b) < 0;
            }
            if overflow {
                return Err(ExprError::NumericOverflow);
            }
            c.set_bitmap(a.null_bitmap() & b.null_bitmap());
            Ok(c)
        })
    });
}
