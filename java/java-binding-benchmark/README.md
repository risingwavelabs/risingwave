# Run the benchmark
1. Build the java binding benchmark
```shell
mvn install --pl java-binding-benchmark --am
```
2. Copy dependency
```shell
mvn dependency:copy-dependencies --pl java-binding-benchmark
```
3. Run benchmark
```shell
java -cp "java-binding-benchmark/target/dependency/*:java-binding-benchmark/target/java-binding-benchmark-1.0-SNAPSHOT.jar" com.risingwave.java.binding.BenchmarkRunner
```