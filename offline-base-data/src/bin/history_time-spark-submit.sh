#!/bin/sh
##"-i /app/data/${currentDay} -share ${currentDay} -cards /app/base/bay_pair/${currentDay}  -vl /app/base/tracker/${currentDay}"
main_class=com.ehl.offline.time.strategy.HistoryAVGTime
currentDay=$1
base_dir=$(dirname $0)/..
cores=3
memory=5g
master=spark://host225:7077
service_name="ehl-offline-history"
executeJar=$base_dir/lib/offline-base-data-1.0-SNAPSHOT.jar
args="/app/base/history_time/${currentDay}"
$base_dir/bin/spark-submit.sh "$base_dir" "$main_class" "$cores" "$memory" "$service_name" "$executeJar" "$args" "${master}"
