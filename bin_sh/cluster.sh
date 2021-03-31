#!/bin/bash

case $1 in
"start"){
	echo " --------- 启动集群 ----------- "
	
	/opt/module/hadoop-2.7.2/sbin/start-dfs.sh
	ssh hadoop103 /opt/module/hadoop-2.7.2/sbin/start-yarn.sh
	
	zk.sh start
	
	sleep 6s
	
	f1.sh start
	
	kf.sh start
	
	f2.sh start
	
	sleep 4s
	nohup /opt/module/hive/bin/hive --service metastore
};;

"stop"){
	echo " --------- 停止集群 ----------- "
	
	f2.sh stop
	
	kf.sh stop
	
	sleep 6s
	
	f1.sh stop
	
	zk.sh stop
	
	ssh hadoop103 /opt/module/hadoop-2.7.2/sbin/stop-yarn.sh
	/opt/module/hadoop-2.7.2/sbin/stop-dfs.sh
};;

esac
