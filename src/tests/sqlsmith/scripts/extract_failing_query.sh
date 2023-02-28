#!/usr/bin/env bash

# USAGE: ./extract_failing_query.sh [LOGPATH] [SQLPATH]
# This script extracts failing query + its ddl + its dml from fuzzing log (LOGPATH),
# writes out to sql script file (SQLPATH).
#
# EXAMPLE: ./extract_failing_query.sh [--pretty] fuzzing-sample.log fuzzing-golden.sql

set -euo pipefail

# -x is unset, since it is verbose.

########## LIBRARY

# Logs are of the form ... [EXECUTING XYZ]: <sql>
# We strip the header, and append ';' to the sql.
extract_sql() {
    sed 's/.*\]: //' | sed 's/$/;/'
}

extract_sqlsmith_logs() {
    grep "risingwave_sqlsmith::runner:" < "$LOGPATH"
}

extract_ddl() {
    grep "\[EXECUTING CREATE" | extract_sql
}

extract_dml() {
    grep "\[EXECUTING POPULATION]" | extract_sql || true
}

# assumes it is last
extract_query() {
    tail -n 1 | extract_sql
}

########## EXTRACT QUERY + DDL + DML

if [ "$1" = "--pretty" ]; then
    LOGPATH="$2"
    SQLPATH="$3"
    PRETTY=1
else
    LOGPATH="$1"
    SQLPATH="$2"
    PRETTY=0
fi

FULL="$(extract_sqlsmith_logs)"
# echo "$FULL"
DDL="$(echo "$FULL" | extract_ddl)"
# echo "$DDL"
DML="$(echo "$FULL" | extract_dml)"
# echo "$DML"
QUERIES="$(echo "$FULL" | extract_query)"
# echo "$QUERIES"

########## STORE 

# Overwrites old logs
if [ "$PRETTY" = 1 ]; then
    echo "Pretty printing to $SQLPATH"
    echo "$DDL" | pg_format > "$SQLPATH"
    # Do not pretty print dml, can be quite verbose
    echo "$DML" >> "$SQLPATH"
    echo "$QUERIES" | pg_format >> "$SQLPATH"
else
    echo "Printing raw output to $SQLPATH"
    echo "$DDL" > "$SQLPATH"
    echo "$DML" >> "$SQLPATH"
    echo "$QUERIES" >> "$SQLPATH"
fi
