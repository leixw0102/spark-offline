#!/bin/sh
base_dir=$(dirname $0)/..

$base_dir/bin/start-often_path.sh >> ${base_dir}/logs/start-ofter-path.log
#wait
##create index
$base_dir/bin/es_index.sh createIndexAndType

##delete
$base_dir/bin/es_index.sh deleteType

#export data to es
$base_dir/bin/start-data2es.sh


