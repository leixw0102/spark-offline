#!/bin/sh

main_class=com.ehl.offline.export.PathOfOftenExport2ES
#currentDay=`date +%Y-%m-%d`

yesterday=$1
if [ "$yesterday" = "" ]
then
  echo "dmin is not set!"
  yesterday=`date -d '-1 day' +%Y-%m-%d`
#else
#  echo "dmin is set !"
#
fi

base_dir=$(dirname $0)/..
cores=3
memory=4g
master=spark://host225:7077
service_name="ehl-path-result-2-es"
executeJar=$base_dir/lib/path-es-1.0-SNAPSHOT.jar
args="/app/pathOfOften/${yesterday}"
$base_dir/bin/es-spark-submit.sh "$base_dir" "$main_class" "$cores" "$memory" "$service_name" "$executeJar" "$args" "${master}"
