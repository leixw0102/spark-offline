#!/bin/sh
inputDay=$1
base_dir=$(dirname $0)/..
##"-i /app/data/${inputDay} -share ${inputDay} -cards /app/base/bay_pair/${inputDay}  -vl /app/base/tracker/${inputDay}"
args="-i /app/data/${inputDay} -share ${inputDay} -cards /app/base/bay_pair/${inputDay}  -vl /app/base/tracker/${inputDay}"
$base_dir/bin/temp-base-spark-submit.sh "${inputDay}" "${args}"
