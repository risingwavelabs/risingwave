# Full-Text Search E2E Tests

Tests in this directory cover full-text indexes created with `USING fulltext`
and queries using `keyword_match`.

## Running Tests

Start a RisingWave cluster with the Hummock state store and at least four compute
slots. No external source or sink services are required. See the
[testing guide](../../docs/dev/src/tests/intro.md) for general test setup.

Run from the repository root:

```sh
./risedev slt './e2e_test/full_text_search/*.slt'
```

## Test Structure

- `local.slt` runs queries in local batch mode.
- `distributed.slt` runs queries in distributed batch mode.
- `full_text_index.slt.part` contains the shared test cases included by both files.

The shared fixture sets streaming parallelism to four and checks the actual
full-text writer fragment parallelism. It also rescales the body index from four
writers to two and back to four, then verifies updates and deletes. Batch query
mode and streaming writer parallelism are tested separately.

## Coverage

- Index creation on text, JSONB, JSONB expressions and materialized views.
- Rejection of unsupported column types and analyzers.
- Case-insensitive token matching and AND semantics for multiple query tokens.
- JSONB string-value matching, excluding object keys and numeric values.
- Inserts, updates, deletes, reinsertion and NULL transitions.
- SQL predicates, ordered queries, unordered LIMIT and comparison with table scans.
- Multiple checkpoint epochs, segment maintenance and writer rescaling.

These tests cover boolean keyword matching, not BM25 scoring or ranked search.
