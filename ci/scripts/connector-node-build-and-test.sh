#!/usr/bin/env bash

set -euo pipefail

source ci/scripts/common.sh

JAVA_VERSION=17

while getopts 'v:' opt; do
    case ${opt} in
        v )
            JAVA_VERSION=$OPTARG
            ;;
        \? )
            echo "Invalid Option: -$OPTARG" 1>&2
            exit 1
            ;;
        : )
            echo "Invalid option: $OPTARG requires an argument" 1>&2
            ;;
    esac
done
shift $((OPTIND -1))

echo "--- Check for changes in java/ and proto/"
if [[ -n "${BUILDKITE_PULL_REQUEST:-}" ]] && [[ "${BUILDKITE_PULL_REQUEST}" != "false" ]]; then
    if [[ "${CI_STEPS:-}" =~ "connector-node-tests" ]]; then
        echo "CI_STEPS forces run. Proceeding."
    else
        # Fetch origin/main to ensure we have the reference for diff
        git fetch origin main || true
        # Check for changes in java/ or proto/ directories
        if git diff --name-only origin/main...HEAD | grep -qE "^(java/|proto/)"; then
            echo "Relevant files changed. Proceeding."
        else
            echo "No changes in java/ or proto/. Skipping tests."
            exit 0
        fi
    fi
fi

echo "--- Install Java $JAVA_VERSION"
apt-get update -y
apt-get install -y "openjdk-$JAVA_VERSION-jdk"

# Set the newly installed Java as default
update-alternatives --set java /usr/lib/jvm/java-$JAVA_VERSION-openjdk-amd64/bin/java
update-alternatives --set javac /usr/lib/jvm/java-$JAVA_VERSION-openjdk-amd64/bin/javac

echo "--- Check Java Version"
java -version

echo "--- Build and Test Connector Node"
# Set TMPDIR to a directory in the workspace to avoid running out of space in /tmp
export TMPDIR=${PWD}/java/tmp
mkdir -p ${TMPDIR}

cd java
mvn --batch-mode --update-snapshots clean package
