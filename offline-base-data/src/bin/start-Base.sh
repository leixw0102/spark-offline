#!/bin/sh
main_class=com.ehl.offline.base.EhlBaseDataProcesser
currentDay=`date +%Y-%m-%d`
base_dir=$(dirname $0)/..
cores=5
memory=10g
service_name="ehl-offline-base"
executeJar=$base_dir/lib/offline-base-data-1.0-SNAPSHOT.jar
args="/app/data/${currentDay} ${currentDay} /app/base/bay_pair/${currentDay}  /app/base/tracker/${currentDay}"
$base_dir/bin/spark-submit.sh "$base_dir" "$main_class" "$cores" "$memory" "$service_name" "$executeJar" "$args"
