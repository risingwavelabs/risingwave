// Copyright 2022 Singularity Data
//
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

use criterion::{criterion_group, criterion_main, Criterion};

criterion_group!(benches, decimal);
criterion_main!(benches);

#[cfg(not(feature = "decimal"))]
fn decimal(_c: &mut Criterion) {}

#[cfg(feature = "decimal")]
fn decimal(c: &mut Criterion) {
    use memcomparable::{Decimal, Deserializer, Serializer};

    // generate decimals
    let mut decimals = vec![];
    for _ in 0..10 {
        decimals.push(Decimal::Normalized(rand::random()));
    }

    c.bench_function("serialize_decimal", |b| {
        let mut i = 0;
        b.iter(|| {
            let mut ser = Serializer::new(vec![]);
            ser.serialize_decimal(decimals[i]).unwrap();
            i += 1;
            if i == decimals.len() {
                i = 0;
            }
        })
    });

    c.bench_function("deserialize_decimal", |b| {
        let encodings = decimals
            .iter()
            .map(|d| {
                let mut ser = Serializer::new(vec![]);
                ser.serialize_decimal(*d).unwrap();
                ser.into_inner()
            })
            .collect::<Vec<_>>();
        let mut i = 0;
        b.iter(|| {
            Deserializer::new(encodings[i].as_slice())
                .deserialize_decimal()
                .unwrap();
            i += 1;
            if i == decimals.len() {
                i = 0;
            }
        })
    });
}
