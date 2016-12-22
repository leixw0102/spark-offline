#!/bin/sh
inputDay=$1
main_class=com.ehl.offline.base.EhlBaseDataProcesser
base_dir=$(dirname $0)/..
cores=5
memory=10g
service_name="ehl-offline-base"
executeJar=$base_dir/lib/offline-base-data-1.0-SNAPSHOT.jar
args="/app/data/${inputDay} ${inputDay} /app/base/bay_pair/${inputDay}  /app/base/tracker/${inputDay}"
$base_dir/bin/spark-submit.sh "$base_dir" "$main_class" "$cores" "$memory" "$service_name" "$executeJar" "$args"
