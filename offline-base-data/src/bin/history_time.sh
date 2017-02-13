#!/bin/sh
base_dir=$(dirname $0)/..
yesterday=`date -d '-1 day' +%Y-%m-%d`
$base_dir/bin/history_time-spark-submit.sh "${yesterday}"
