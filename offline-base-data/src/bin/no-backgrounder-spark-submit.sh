#!/bin/sh
# loading dependency jar in lib directory

base_dir=$1
main_class=$2
cores=$3
memory=$4
service_name=$5
executeJar=$6
args=$7

for file in $base_dir/lib/*.jar;
do
    #if [ "$file" != "$base_dir/lib/offline-base-1.0-SNAPSHOT.jar" ]; then
      JARS=$JARS,$file
    #fi
done
  JARS=${JARS:1}

if [ -z "$BASE_OPTS" ]; then
  BASE_OPTS="-Dbase-conf=$base_dir/conf/base.conf"
fi

java_options="$BASE_OPTS "
#standalone
master=$8
#yarn clustor
#master=yarn
#--deploy-mode=client


cmd="spark-submit\
 --master ${master}\
 --class ${main_class}\
 --total-executor-cores ${cores}\
 --executor-memory ${memory}\
 --jars ${JARS}\
 --driver-java-options \"${java_options}\"\
 ${executeJar}\
 ${args}"

$base_dir/bin/no-backgrounder-daemon.sh start "$cmd" "$service_name"