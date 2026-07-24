# Release dependency freeze

The connector follows TapData's runtime-compatible `2.0.8-SNAPSHOT` PDK/API convention. For the
`1.0.0` release, the required TapData artifacts resolve to timestamped build
`20260624.021218-4`. `tapdata-2.0.8-20260624.021218-4.sha256` records the complete TapData dependency
subset used by the build: four JARs and nine POMs, with paths relative to Maven's
`io/tapdata` directory.

Verify an isolated dependency repository and build offline:

```bash
workspace=$PWD
release_repo=$(mktemp -d)

mvn -B -U -f tapdata-plugin/pom.xml \
  -Dmaven.repo.local="$release_repo" \
  org.apache.maven.plugins:maven-dependency-plugin:3.7.0:go-offline

(cd "$release_repo/io/tapdata" && \
  shasum -a 256 -c \
    "$workspace/tapdata-plugin/release/tapdata-2.0.8-20260624.021218-4.sha256")

mvn -B -o -f tapdata-plugin/pom.xml \
  -Dmaven.repo.local="$release_repo" clean verify
```

Set `JAVA_HOME` to the release builder's JDK 17 installation and record `java -version` plus
`mvn -version`. Do not embed machine-specific JDK paths in release instructions.

The checksum manifest detects dependency drift but cannot restore artifacts if TapData's Nexus
prunes the timestamped snapshot. Archive the verified dependency subset with the release.

Build the distributable JAR only after all source changes are committed:

```bash
test -z "$(git status --porcelain)"
mvn -B -o -f tapdata-plugin/pom.xml clean package

unzip -p tapdata-plugin/target/risingwave-connector-1.0.0.jar META-INF/MANIFEST.MF \
  | grep -E '^(Git-Commit|Git-Dirty):'
git rev-parse HEAD
```

Reject a candidate if `Git-Dirty` is not `false` or `Git-Commit` differs from the reviewed source
revision. After exact-JAR qualification, write its SHA-256 to
`release/risingwave-connector-1.0.0.sha256` and complete the final artifact record in
`TAPDATA_RISINGWAVE_PRODUCTION_READINESS.md`.

The JAR embeds a build timestamp and is not byte-for-byte reproducible. Distribute and archive one
canonical file; never replace it with a same-version rebuild.
