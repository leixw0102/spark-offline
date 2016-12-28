#!/bin/sh
inputDay=$1
base_dir=$(dirname $0)/..
$base_dir/bin/base-spark-submit.sh ${inputDay}
