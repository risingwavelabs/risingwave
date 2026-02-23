# AGENTS.md - Java Binding Benchmarks

## 1. Scope

Policies for the `java/java-binding-benchmark` directory, covering JMH-based performance benchmarks for Java language bindings.

## 2. Purpose

The Java Binding Benchmarks module provides performance testing infrastructure using the Java Microbenchmark Harness (JMH). It measures the performance of Java-Rust JNI operations, stream chunk processing, and collection operations to identify bottlenecks and validate performance improvements.

## 3. Structure

```
java/java-binding-benchmark/
├── pom.xml                    # Maven project configuration
├── README.md                  # Build and run instructions
├── src/
│   └── main/java/com/risingwave/java/binding/
│       ├── BenchmarkRunner.java    # JMH benchmark runner
│       ├── ArrayListBenchmark.java # ArrayList performance tests
│       └── StreamchunkBenchmark.java   # Stream chunk benchmarks
└── target/                    # Build output (gitignored)
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `pom.xml` | Maven configuration with JMH dependencies |
| `README.md` | Build and execution instructions |
| `BenchmarkRunner.java` | Main entry point for running benchmarks |
| `ArrayListBenchmark.java` | Collection operation benchmarks |
| `StreamchunkBenchmark.java` | Stream chunk processing benchmarks |

## 5. Edit Rules (Must)

- Run `mvn spotless:apply` before committing
- Follow JMH best practices for accurate measurements
- Document benchmark purpose and methodology
- Use appropriate JMH annotations (@Benchmark, @Fork, etc.)
- Warm up iterations before measurement
- Report results in standard JMH format
- Ensure benchmarks are reproducible

## 6. Forbidden Changes (Must Not)

- Add benchmarks without warm-up iterations
- Use System.nanoTime() directly instead of JMH primitives
- Benchmark code without accounting for JVM optimizations
- Commit benchmark results to repository
- Add non-deterministic benchmark operations

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Build benchmark | `mvn install --pl java-binding-benchmark --am` |
| Copy dependencies | `mvn dependency:copy-dependencies --pl java-binding-benchmark` |
| Run benchmarks | `java -cp "java-binding-benchmark/target/dependency/*:java-binding-benchmark/target/java-binding-benchmark-*.jar" com.risingwave.java.binding.BenchmarkRunner` |
| Spotless check | `mvn spotless:check -pl java-binding-benchmark` |

## 8. Dependencies & Contracts

- Java 11+
- Maven 3.8+
- JMH 1.37+ (core and annotation processor)
- risingwave-java-binding (JNI library)
- JUnit 4.13.2 for test scaffolding

## 9. Overrides

Inherits from `/home/k11/risingwave/java/AGENTS.md`:
- Override: Edit Rules - JMH-specific requirements
- Override: Test Entry - Benchmark execution commands

## 10. Update Triggers

Regenerate this file when:
- New benchmark classes are added
- JMH configuration changes
- Benchmark methodology evolves
- New performance metrics are tracked

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: /home/k11/risingwave/java/AGENTS.md
