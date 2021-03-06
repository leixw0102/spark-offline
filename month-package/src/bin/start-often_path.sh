#!/bin/sh
main_class=com.ehl.offline.month.EhlPathOfOftenBusinessSpark
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

#yesterday=`date -d '-1 day' +%Y-%m-%d`
base_dir=$(dirname $0)/..
cores=5
memory=10g
master=spark://host51:7077
service_name="ehl-offline-path-often"
executeJar=$base_dir/lib/offline-month-business-1.0-SNAPSHOT.jar
args="/app/pathOfOften/${yesterday}"
$base_dir/bin/path-spark-submit.sh "$base_dir" "$main_class" "$cores" "$memory" "$service_name" "$executeJar" "$args" "${master}"
