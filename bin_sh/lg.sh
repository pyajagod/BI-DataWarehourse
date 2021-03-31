#! /bin/bash

	for i in hadoop102 hadoop103 
	do
		echo ---------$i 生成日志 -------------
		ssh $i "java -classpath /opt/module/logcollector-1.0-SNAPSHOT-jar-with-dependencies.jar com.qwy.appclient.AppMain $1 $2 >/dev/null 2>&1 &"
	done

