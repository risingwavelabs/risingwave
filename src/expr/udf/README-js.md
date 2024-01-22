# Use UDFs in JavaScript

This article provides a step-by-step guide for defining JavaScript functions in RisingWave.

JavaScript code is inlined in `CREATE FUNCTION` statement and then run on the embedded QuickJS virtual machine in RisingWave. It does not support access to external networks and is limited to computational tasks only.
Compared to other languages, JavaScript UDFs offer the easiest way to define UDFs in RisingWave.

## Define your functions

You can use the `CREATE FUNCTION` statement to create JavaScript UDFs. The syntax is as follows:

```sql
CREATE FUNCTION function_name ( arg_name arg_type [, ...] )
    [ RETURNS return_type | RETURNS TABLE ( column_name column_type [, ...] ) ]
    LANGUAGE javascript
    AS [ $$ function_body $$ | 'function_body' ];
```

The argument names you define can be used in the function body. For example:

```sql
CREATE FUNCTION gcd(a int, b int) RETURNS int LANGUAGE javascript AS $$
    if(a == null || b == null) {
        return null;
    }
    while (b != 0) {
        let t = b;
        b = a % b;
        a = t;
    }
    return a;
$$;
```

The correspondence between SQL types and JavaScript types can be found in the [appendix table](#appendix-type-mapping). You need to ensure that the type of the return value is either `null` or consistent with the type in the `RETURNS` clause.

If the function you define returns a table, you need to use the `yield` statement to return the data of each row. For example:

```sql
CREATE FUNCTION series(n int) RETURNS TABLE (x int) LANGUAGE javascript AS $$
    for(let i = 0; i < n; i++) {
        yield i;
    }
$$;
```

## Use your functions

Once the UDFs are created in RisingWave, you can use them in SQL queries just like any built-in functions. For example:

```sql
SELECT gcd(25, 15);
SELECT * from series(5);
```

## Appendix: Type Mapping

The following table shows the type mapping between Arrow and JavaScript:

| Arrow Type            | JS Type       | Note                  |
| --------------------- | ------------- | --------------------- |
| Null                  | null          |                       |
| Boolean               | boolean       |                       |
| Int8                  | number        |                       |
| Int16                 | number        |                       |
| Int32                 | number        |                       |
| Int64                 | number        |                       |
| UInt8                 | number        |                       |
| UInt16                | number        |                       |
| UInt32                | number        |                       |
| UInt64                | number        |                       |
| Float32               | number        |                       |
| Float64               | number        |                       |
| Utf8                  | string        |                       |
| Binary                | Uint8Array    |                       |
| LargeString (json)    | null, boolean, number, string, array or object | `JSON.parse(string)`  |
| LargeBinary (decimal) | BigDecimal    |                       |
| List(Int8)            | Int8Array     |                       |
| List(Int16)           | Int16Array    |                       |
| List(Int32)           | Int32Array    |                       |
| List(Int64)           | BigInt64Array |                       |
| List(UInt8)           | Uint8Array    |                       |
| List(UInt16)          | Uint16Array   |                       |
| List(UInt32)          | Uint32Array   |                       |
| List(UInt64)          | BigUint64Array|                       |
| List(Float32)         | Float32Array  |                       |
| List(Float64)         | Float64Array  |                       |
| List(others)          | Array         |                       |
| Struct                | object        |                       |
