This is a program used for e2e test in extended mode.

## What is difference between this and `e2e_test/extended_mode`

For e2e test in extended query mode, there are a few things we can't test in sqllogictest

1. bind parameter
2. max row number
3. cancel query

See more details [here](https://www.postgresql.org/docs/15/protocol-flow.html#PROTOCOL-FLOW-PIPELINING:~:text=Once%20a%20portal,count%20is%20ignored).

Before sqllogictest supports these, we test these functions in this program. In the future, we may merge it to `e2e_test/extended_mode`.

# How to run

```shell
RUST_BACKTRACE=1 target/debug/risingwave_e2e_extended_mode_test --host 127.0.0.1 \
  -p 4566 \
  -u root \
  --database dev \
```