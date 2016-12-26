#!/bin/sh


inputDay=$1
days=$2

for i in `seq 0 $2`
do
   file_name=`date +%Y-%m-%d`
   s= date -d "+$i day ${file_name}" +%Y-%m-%d
   echo $s
   main_class=com.ehl.offline.base.EhlBaseDataProcesser
    base_dir=$(dirname $0)/..
    cores=5
    memory=10g
    service_name="ehl-offline-base"
    executeJar=$base_dir/lib/offline-base-data-1.0-SNAPSHOT.jar
    args="/app/data/${inputDay} ${inputDay} /app/base/bay_pair/${inputDay}  /app/base/tracker/${inputDay}"
    $base_dir/bin/spark-submit.sh "$base_dir" "$main_class" "$cores" "$memory" "$service_name" "$executeJar" "$args"
done


