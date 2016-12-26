#!/bin/sh


inputDay=`date +%Y-%m-%d`
days=30

for i in `seq 0 $2`
do
   file_name=`date +%Y-%m-%d`
   s= date -d "+$i day ${file_name}" +%Y-%m-%d
   echo $s
done


