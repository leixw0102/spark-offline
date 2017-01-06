#!/bin/sh

script_path=$(cd "$(dirname "$0")"; pwd)
base=$(dirname ${script_path})
$base/bin/no-backgrounder-daemon.sh status
