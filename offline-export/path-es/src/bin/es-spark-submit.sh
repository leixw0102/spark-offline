#!/bin/sh
# loading dependency jar in lib directory
currentDay=`date +%Y-%m-%d %H:%M:%S`
base_dir=$1
main_class=$2
cores=$3
memory=$4
service_name=$5" ${currentDay}"
executeJar=$6
args=$7

for file in $base_dir/lib/*.jar;
do
    #if [ "$file" != "$base_dir/lib/offline-base-1.0-SNAPSHOT.jar" ]; then
      JARS=$JARS,$file
    #fi
done
  JARS=${JARS:1}

if [ -z "$ES_OPTS" ]; then
  ES_OPTS="-Dbase-conf=$base_dir/conf/es.conf"
fi




java_options="$ES_OPTS "
#standalone
master=spark://host236.ehl.com:7077
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

$base_dir/bin/daemon.sh start "$cmd" "$service_name"