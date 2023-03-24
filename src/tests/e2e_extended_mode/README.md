This is a program used for e2e test in extended mode.

## What is difference between it and extended_mode/*.slt in e2e_test

For e2e test in extended query mode, there are two thing we can't test in sqllogitest
1. bind parameter 
2. max row number 
See [detail](https://www.postgresql.org/docs/15/protocol-flow.html#PROTOCOL-FLOW-PIPELINING:~:text=Once%20a%20portal,count%20is%20ignored)

So before sqllogictest supporting these, we test these function in this program. 

In the future, we may merge it to e2e_text/extended_query

# How to run

```shell
RUST_BACKTRACE=1 target/debug/risingwave_e2e_extended_mode_test --host 127.0.0.1 \
  -p 4566 \
  -u root \
  --database dev \
```