#!/bin/sh

main_class=com.ehl.offline.export.PathOfOftenExport2ES
currentDay=`date +%Y-%m-%d`
base_dir=$(dirname $0)/..
cores=3
memory=4g
service_name="ehl-path-result-2-es"
executeJar=$base_dir/lib/path-es-1.0-SNAPSHOT.jar
args="/app/pathOfOften/${currentDay}"
$base_dir/bin/es-spark-submit.sh "$base_dir" "$main_class" "$cores" "$memory" "$service_name" "$executeJar" "$args"
