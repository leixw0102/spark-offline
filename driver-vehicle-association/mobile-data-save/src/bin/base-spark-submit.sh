#!/bin/sh
##"-i /app/data/${currentDay} -share ${currentDay} -cards /app/base/bay_pair/${currentDay}  -vl /app/base/tracker/${currentDay}"
main_class=$1
#currentDay=$1
base_dir=$(dirname $0)/..
cores=5
memory=10g
master=spark://host225:7077
service_name="ehl-save-hbase"
executeJar=$base_dir/lib/mobile-data-save-1.0-SNAPSHOT.jar
args=$2
$base_dir/bin/spark-submit.sh "$base_dir" "$main_class" "$cores" "$memory" "$service_name" "$executeJar" "$args" "${master}"
