#!/bin/bash
for i in hadoop102 hadoop103 hadoop104
do
	echo ----------- $i 同步时间 ------------
	ssh -t $i  "sudo date -s $1 "
done
