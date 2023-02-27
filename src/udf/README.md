# Python UDF Support

🚧 Working in progress.

# Usage

```sh
pip3 install pyarrow
# run server
python3 python/example.py
# run client (test client for the arrow flight UDF client-server protocol)
cargo run --example client
```

Risingwave client:

```sql
dev=> create function gcd(int, int) returns int as 'http://localhost:8815' language arrow_flight;
dev=> select gcd(25, 15);
```
