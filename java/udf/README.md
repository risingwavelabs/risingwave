# RisingWave Java UDF SDK

This library provides a Java SDK for creating user-defined functions (UDF) in RisingWave.

## Introduction

RisingWave supports user-defined functions implemented as external functions.
With the RisingWave Java UDF SDK, users can define custom UDFs using Java and start a Java process as a UDF server.
RisingWave can then remotely access the UDF server to execute the defined functions.

## Installation

To install the RisingWave Java UDF SDK:

```sh
git clone https://github.com/risingwavelabs/risingwave.git
cd risingwave/java/udf
mvn install
```

## Creating a New Project

> NOTE: You can also start from the [udf-example](../udf-example) project without creating the project from scratch.

To create a new project using the RisingWave Java UDF SDK, follow these steps:

```sh
mvn archetype:generate -DgroupId=com.example -DartifactId=udf-example -DarchetypeArtifactId=maven-archetype-quickstart -DarchetypeVersion=1.4 -DinteractiveMode=false
```

Configure your `pom.xml` file as follows:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>
    <groupId>com.example</groupId>
    <artifactId>udf-example</artifactId>
    <version>1.0-SNAPSHOT</version>

    <dependencies>
        <dependency>
            <groupId>com.risingwave.java</groupId>
            <artifactId>risingwave-udf</artifactId>
            <version>0.0.1</version>
        </dependency>
    </dependencies>
</project>
```

The `--add-opens` flag must be added when running unit tests through Maven:

```xml
<build>
    <plugins>
        <plugin>
            <groupId>org.apache.maven.plugins</groupId>
            <artifactId>maven-surefire-plugin</artifactId>
            <version>3.0.0-M7</version>
            <configuration>
                <argLine>--add-opens=java.base/java.nio=ALL-UNNAMED</argLine>
            </configuration>
        </plugin>
    </plugins>
</build>
```

## Scalar Functions

A user-defined scalar function maps zero, one, or multiple scalar values to a new scalar value. 

In order to define a scalar function, one has to create a new class that implements the `ScalarFunction`
interface in `com.risingwave.functions` and implement exactly one evaluation method named `eval(...)`.
This method must be declared public and non-static.

Any [data type](#data-types) listed in the data types section can be used as a parameter or return type of an evaluation method.

Here's an example of a scalar function that calculates the greatest common divisor (GCD) of two integers:

```java
import com.risingwave.functions.ScalarFunction;

public class Gcd implements ScalarFunction {
    public int eval(int a, int b) {
        while (b != 0) {
            int temp = b;
            b = a % b;
            a = temp;
        }
        return a;
    }
}
```

> **NOTE:** Differences with Flink
> 1. The `ScalarFunction` is an interface instead of an abstract class.
> 2. Multiple overloaded `eval` methods are not supported.
> 3. Variable arguments such as `eval(Integer...)` are not supported.

## Table Functions

A user-defined table function maps zero, one, or multiple scalar values to one or multiple
rows (structured types). 

In order to define a table function, one has to create a new class that implements the `TableFunction`
interface in `com.risingwave.functions` and implement exactly one evaluation method named `eval(...)`. 
This method must be declared public and non-static.

The return type must be an `Iterator` of any [data type](#data-types) listed in the data types section.
Similar to scalar functions, input and output data types are automatically extracted using reflection.
This includes the generic argument T of the return value for determining an output data type.

Here's an example of a table function that generates a series of integers:

```java
import com.risingwave.functions.TableFunction;

public class Series implements TableFunction {
    public Iterator<Integer> eval(int n) {
        return java.util.stream.IntStream.range(0, n).iterator();
    }
}
```

> **NOTE:** Differences with Flink
> 1. The `TableFunction` is an interface instead of an abstract class. It has no generic arguments.
> 2. Instead of calling `collect` to emit a row, the `eval` method returns an `Iterator` of the output rows.
> 3. Multiple overloaded `eval` methods are not supported.
> 4. Variable arguments such as `eval(Integer...)` are not supported.
> 5. In SQL, table functions can be used in the `FROM` clause directly. `JOIN LATERAL TABLE` is not supported.

## UDF Server

To create a UDF server and register functions:

```java
import com.risingwave.functions.UdfServer;

public class App {
    public static void main(String[] args) {
        try (var server = new UdfServer("0.0.0.0", 8815)) {
            // register functions
            server.addFunction("gcd", new Gcd());
            server.addFunction("series", new Series());
            // start the server
            server.start();
            server.awaitTermination();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
```

To run the UDF server, execute the following command:

```sh
_JAVA_OPTIONS="--add-opens=java.base/java.nio=ALL-UNNAMED" mvn exec:java -Dexec.mainClass="com.example.App"
```

## Creating Functions in RisingWave

```sql
create function gcd(int, int) returns int
language java as gcd using link 'http://localhost:8815';

create function series(int) returns table (x int)
language java as series using link 'http://localhost:8815';
```

For more detailed information and examples, please refer to the official RisingWave [documentation](https://www.risingwave.dev/docs/current/user-defined-functions/#4-declare-your-functions-in-risingwave).

## Using Functions in RisingWave

Once the user-defined functions are created in RisingWave, you can use them in SQL queries just like any built-in functions. Here are a few examples:

```sql
select gcd(25, 15);

select * from series(10);
```

## Data Types

The RisingWave Java UDF SDK supports the following data types:

| SQL Type  | Java Type           | Notes              |
| --------- | ------------------- | ------------------ |
| BOOLEAN   | boolean, Boolean    |                    |
| SMALLINT  | short, Short        |                    |
| INT       | int, Integer        |                    |
| BIGINT    | long, Long          |                    |
| REAL      | float, Float        |                    |
| DOUBLE PRECISION | double, Double      |                    |
| DECIMAL   | BigDecimal          |                    |
| DATE      | java.time.LocalDate |                    |
| TIME      | java.time.LocalTime |                    |
| TIMESTAMP | java.time.LocalDateTime |                    |
| INTERVAL  | com.risingwave.functions.PeriodDuration |                    |
| VARCHAR   | String              |                    |
| BYTEA     | byte[]              |                    |
| JSONB     | String              | Use `@DataTypeHint("JSONB") String` as the type. See [example](#jsonb). |
| JSONB[]   | String[]            | Use `@DataTypeHint("JSONB[]") String[]` as the type. |
| STRUCT<>  | user-defined class  | Define a data class as the type. See [example](#struct-type). |
| ...others |                     | Not supported yet. |

### JSONB

```java
import com.google.gson.Gson;

// Returns the i-th element of a JSON array.
public class JsonbAccess implements ScalarFunction {
    static Gson gson = new Gson();

    public @DataTypeHint("JSONB") String eval(@DataTypeHint("JSONB") String json, int index) {
        if (json == null)
            return null;
        var array = gson.fromJson(json, Object[].class);
        if (index >= array.length || index < 0)
            return null;
        var obj = array[index];
        return gson.toJson(obj);
    }
}
```

```sql
create function jsonb_access(jsonb, int) returns jsonb
language java as jsonb_access using link 'http://localhost:8815';
```

### Struct Type

```java
// Split a socket address into host and port.
public static class IpPort implements ScalarFunction {
    public static class SocketAddr {
        public String host;
        public short port;
    }

    public SocketAddr eval(String addr) {
        var socketAddr = new SocketAddr();
        var parts = addr.split(":");
        socketAddr.host = parts[0];
        socketAddr.port = Short.parseShort(parts[1]);
        return socketAddr;
    }
}
```

```sql
create function ip_port(varchar) returns struct<host varchar, port smallint>
language java as ip_port using link 'http://localhost:8815';
```

## Full Example

You can checkout [udf-example](../udf-example) and use it as a template to create your own UDFs.
