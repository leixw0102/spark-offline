#!/bin/sh
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


main=com.ehl.offline.base.EhlBaseDataProcesser
args="-i /app/data/${yesterday} -share ${yesterday} -cards /app/base/bay_pair/${yesterday}"
##  -vl /app/base/tracker/${yesterday}"
$base_dir/bin/base-spark-submit.sh  "${main}" "${args}"
