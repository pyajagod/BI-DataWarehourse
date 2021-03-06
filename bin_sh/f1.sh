#!/bin/bash

case $1 in
"start"){
	for i in hadoop102 hadoop103
	do
		echo ----------- $i flume启动 --------------
		ssh $i "nohup /opt/module/flume/bin/flume-ng agent --conf-file /opt/module/flume/conf/file-flume-kafka.conf --name a1 -Dflume.root.logger=INFO,LOGFILE >/opt/module/flume/test1 2>&1  &"
	done
};;

"stop"){
	for i in hadoop102 hadoop103
	do
		echo ----------- $i flume停止 --------------
		ssh $i "ps -ef | grep file-flume-kafka | grep -v grep |awk  '{print \$2}' | xargs kill"
	done
};;
esac
