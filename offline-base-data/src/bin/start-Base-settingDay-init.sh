#!/bin/sh

base_dir=$(dirname $0)/..
inputDay=$1
days=$2

for i in `seq 0 ${days}`
do
   file_name=${inputDay}
   s=`date -d '-${i} day' +%Y-%m-%d`
   echo $s
    $base_dir/bin/no-backgrounder-base-spark-submit ${s}
done

#! /bin/bash
first=$1
second=$2

while [ "$first" != "$second" ]
do
echo $first
let first=`date -d "-1 days ago ${first}" +%Y%m%d`
done


