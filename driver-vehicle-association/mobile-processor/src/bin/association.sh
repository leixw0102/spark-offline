#!/bin/sh
##"-i /app/data/${currentDay} -share ${currentDay} -cards /app/base/bay_pair/${currentDay}  -vl /app/base/tracker/${currentDay}"
main_class=com.ehl.mobile.processor.AssociationSpark
base_dir=$(dirname $0)/..
yesterday=$1
if [ "$yesterday" = "" ]
then
  echo "dmin is not set!"
  yesterday=`date -d '-1 day' +%Y-%m-%d`
#else
#  echo "dmin is set !"
#
fi

cores=3
memory=5g
master=spark://host10:7077
service_name="mobile-association"
executeJar=$base_dir/lib/mobile-processer-1.0-SNAPSHOT.jar
args="${yesterday}"
$base_dir/bin/spark-submit.sh "$base_dir" "$main_class" "$cores" "$memory" "$service_name" "$executeJar" "$args" "${master}"
