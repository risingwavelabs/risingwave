#!/usr/bin/env bash

export CLASSPATH
find "$HADOOP_HOME" -iname "*.jar" -print0 | xargs -0 echo | tr ' ' ':' > temp_classpath
CLASSPATH=$(cat temp_classpath):${CLASSPATH}
rm temp_classpath
export CLASSPATH=$HADOOP_CONF_DIR:$CLASSPATH

/risingwave/bin/risingwave "$@"
