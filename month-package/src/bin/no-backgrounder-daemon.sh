#!/bin/sh


#. /etc/init.d/functions
#绝对路径
SCRIPT_PATH=$(cd "$(dirname "$0")"; pwd)
BASE=$(dirname ${SCRIPT_PATH})
SERVICE_NAME=$3
SERVICE_PID=${SERVICE_NAME}.pid
PID_PATH_NAME=${BASE}/$SERVICE_PID
CMD=$2


#echo "base: $BASE"
#echo "pid path name: $PID_PATH_NAME"

# Start service 
start() {
    echo "Starting $SERVICE_NAME ..."

    if [ -f "$PID_PATH_NAME" ]; then
    	PID=$(cat $PID_PATH_NAME);
    else
	    PID=0;
    fi
    if [ ! -d "${BASE}/logs" ]; then
    	mkdir  ${BASE}/logs
    fi
    #if [ ! -d "/proc/$PID" ]; then
        echo -e "running command \n$CMD\n"
        eval nohup $CMD  >${BASE}/logs/${SERVICE_PID}.log

        #pid=$!
        #sleep 1

        #if [ ! -d "/proc/$pid" ]; then
         #   echo "$SERVICE_NAME not started. See log for detail."
         #   rm -f $PID_PATH_NAME
         #   rm -f /var/lock/subsys/$SERVICE_NAME
         #   exit 1
        #else
        #    echo $pid > $PID_PATH_NAME
        ##    echo "$SERVICE_NAME started ... PID: $!"
        #    touch /var/lock/subsys/$SERVICE_NAME
        #fi
    #else
    #    echo "$SERVICE_NAME is already running ..."
    #fi
}

# Start service 
stop() {
    if [ -f "$PID_PATH_NAME" ]; then
    	PID=$(cat $PID_PATH_NAME);
    else
	    PID=0;
    fi

    if [ -d "/proc/$PID" ]; then
        echo "$SERVICE_NAME stoping ..."
        for p_pid in `ps -ef |grep $PID|egrep -v grep | awk '{print $2}'`
        do
         kill -s 9 $p_pid
        done
        echo "$SERVICE_NAME stopped ..."
        rm -f $PID_PATH_NAME
        rm -f /var/lock/subsys/$SERVICE_NAME
    else
        echo "$SERVICE_NAME is not running."
    fi
}

status() {
    if [ -f "$PID_PATH_NAME" ]; then
    	PID=$(cat $PID_PATH_NAME);
    else
	    PID=0;
    fi
    #echo $PID;

    if [ -d "/proc/$PID" ]; then
        echo "Service '$SERVICE_NAME' is running..."
	echo "PID: $PID"
    else
        echo "Service '$SERVICE_NAME' is not running."
    fi
}

case $1 in
    start)
        start
        ;;
    stop)
        stop
        ;;
    restart)
        stop
        start
        ;;
    status)
        status
        ;;
    *)
        echo $"Usage: $0 {start|stop|restart|reload|status}"
        exit 1
esac

exit 0
