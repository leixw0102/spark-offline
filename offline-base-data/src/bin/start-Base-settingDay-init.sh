#!/bin/sh

base_dir=$(dirname $0)/..
inputDay=$1
days=$2

for i in `seq 0 ${days}`
do
   file_name=${inputDay}
   s=`date -d '-${i} day' +%Y-%m-%d`
   echo $s
    $base_dir/bin/base-spark-submit ${s}
done


