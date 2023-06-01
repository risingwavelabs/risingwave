## How to run example

Make sure you have installed Java 11 and Maven 3 or later.

```sh
_JAVA_OPTIONS="--add-opens=java.base/java.nio=ALL-UNNAMED" mvn exec:java -Dexec.mainClass="com.risingwave.functions.example.UdfExample"
```
