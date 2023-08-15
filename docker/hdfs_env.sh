#!/usr/bin/env bash

export CLASSPATH=$(find $HADOOP_HOME -iname "*.jar" | xargs echo | tr ' ' ':'):${CLASSPATH}
export CLASSPATH=$HADOOP_CONF_DIR:$CLASSPATH

/risingwave/bin/risingwave "$@"
