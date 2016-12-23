#!/bin/sh
base_dir=$(dirname $0)/..

$base_dir/bin/start-often_path.sh
wait
##create index
$base_dir/bin/es_index.sh createIndexAndType
#create type
$base_dir/bin/es_index.sh createType

#export data to es
$base_dir/bin/start-data2es.sh
