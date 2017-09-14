#!/bin/sh
base_dir=$(dirname $0)/..
zk=host130:2181
apache_phoenix_path=/app/apache-phoenix-4.9.0-HBase-1.2-bin
$apache_phoenix_path/bin/psql.py $zk $base_dir/conf/secondary_index.sql
