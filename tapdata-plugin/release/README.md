# Release dependency freeze

The RisingWave connector follows TapData's runtime-compatible snapshot convention for the
provided PDK dependencies. For the `1.0.0` release, all TapData snapshot artifacts resolve to
timestamped build `20260624.021218-4`.

`tapdata-2.0.8-20260624.021218-4.sha256` lists the complete TapData dependency subset used by the
build: four JARs and nine POMs. Paths are relative to the Maven repository's `io/tapdata`
directory.

Verify an isolated repository before an offline release build:

```bash
workspace=$PWD
release_repo=$(mktemp -d)

mvn -B -U -f tapdata-plugin/pom.xml \
  -Dmaven.repo.local="$release_repo" \
  org.apache.maven.plugins:maven-dependency-plugin:3.7.0:go-offline

(cd "$release_repo/io/tapdata" && \
  shasum -a 256 -c \
    "$workspace/tapdata-plugin/release/tapdata-2.0.8-20260624.021218-4.sha256")

# macOS/Homebrew OpenJDK 17 example:
JAVA_HOME=/opt/homebrew/opt/openjdk@17 \
PATH=/opt/homebrew/opt/openjdk@17/bin:$PATH \
mvn -B -o -f tapdata-plugin/pom.xml \
  -Dmaven.repo.local="$release_repo" clean verify
```

The `/opt/homebrew/...` paths are only a macOS/Homebrew example. On Linux or another macOS JDK
installation, set `JAVA_HOME` to that environment's JDK 17 home and prepend its `bin` directory to
`PATH` before running the same Maven command (for example, many Debian/Ubuntu systems install it
under `/usr/lib/jvm/java-17-openjdk-amd64`). Confirm the selected runtime with `java -version` and
`mvn -version`; do not assume either example path exists on the release builder.

The checksum manifest detects dependency drift but cannot restore artifacts if TapData's Nexus
prunes the timestamped snapshot. Archive the verified dependency subset with the release in an
internal immutable artifact store.

The connector JAR embeds a build timestamp, so this process freezes dependency identity rather
than making the JAR byte-for-byte reproducible. Distribute one canonical JAR and record its SHA-256,
Maven version, JDK version, and Git commit in the production-readiness record.

The build also writes `Git-Commit` and `Git-Dirty` into the shaded JAR manifest. Build a release
only from a clean committed worktree, and reject any candidate whose manifest reports
`Git-Dirty: true` or whose `Git-Commit` differs from the reviewed source revision:

```bash
test -z "$(git status --porcelain)"

JAVA_HOME=/opt/homebrew/opt/openjdk@17 \
PATH=/opt/homebrew/opt/openjdk@17/bin:$PATH \
mvn -B -o -f tapdata-plugin/pom.xml clean package

unzip -p tapdata-plugin/target/risingwave-connector-1.0.0.jar META-INF/MANIFEST.MF \
  | grep -E '^(Git-Commit|Git-Dirty):'
git rev-parse HEAD
```

The JDK path remains an environment-specific example. The release sequence is: commit source,
build and qualify the JAR from that clean commit, then commit only its checksum and qualification
record. The later documentation commit does not change the source revision embedded in the JAR.

For the qualified `1.0.0` release candidate built on 2026-07-17, verify the canonical JAR from the
module directory before distribution:

```bash
cd tapdata-plugin
shasum -a 256 -c release/risingwave-connector-1.0.0.sha256
```

The checksum is valid only for the recorded canonical file. A rebuild has a different build
timestamp and must be qualified and checksummed as a new artifact.
