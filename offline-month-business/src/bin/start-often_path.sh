#!/bin/sh
main_class=com.ehl.offline.month.EhlPathOfOftenBusinessSpark
currentDay=`date +%Y-%m-%d`
base_dir=$(dirname $0)/..
cores=5
memory=10g
service_name="ehl-offline-path-often"
executeJar=$base_dir/lib/offline-month-business-1.0-SNAPSHOT.jar
args="/app/pathOfOften/${currentDay}"
$base_dir/bin/path-spark-submit.sh "$base_dir" "$main_class" "$cores" "$memory" "$service_name" "$executeJar" "$args"
