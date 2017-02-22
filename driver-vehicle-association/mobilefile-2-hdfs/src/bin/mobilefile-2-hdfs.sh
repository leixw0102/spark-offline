#!/bin/sh
base_dir=$(dirname $0)/..

# loading dependency jar in lib directory
for file in $base_dir/lib/*.jar;
do
  CLASSPATH=$CLASSPATH:$file
done

echo $CLASSPATH

if [ -z "$CONFIG_OPTS" ]; then
  CONFIG_OPTS="-Dmobile-config=$base_dir/conf/base.conf"
fi

#if [ -z "$PUBLISH_KAFKA_OPTS" ]; then
#  PUBLISH_KAFKA_OPTS="-Dpublish-kafka=$base_dir/config/kafka-producer.properties "
#fi
# Which java to use
if [ -z "$JAVA_HOME" ]; then
  JAVA="java"
else
  JAVA="$JAVA_HOME/bin/java"
fi

java $CONFIG_OPTS  -classpath $CLASSPATH  com.ehl.offline.FileUnionAction
